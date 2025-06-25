public class AccountFeedProcessor {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    public AccountFeedProcessor(JdbcTemplate jdbcTemplate, PlatformTransactionManager txManager) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(txManager);
    }

    public void processInChunks(List<FeedRecord> feed, int chunkSize) {
        List<List<FeedRecord>> chunks = partition(feed, chunkSize);
        for (List<FeedRecord> chunk : chunks) {
            transactionTemplate.executeWithoutResult(status -> {
                LinkedHashMap<String, List<FeedRecord>> grouped = groupByAccount(chunk);
                for (Map.Entry<String, List<FeedRecord>> entry : grouped.entrySet()) {
                    for (FeedRecord record : entry.getValue()) {
                        try {
                            processRecord(record);
                        } catch (DuplicateKeyException e) {
                            logDuplicate(record);
                        } catch (Exception e) {
                            logError(record, e);
                            throw e; // rollback the chunk
                        }
                    }
                }
            });
        }
    }

    private LinkedHashMap<String, List<FeedRecord>> groupByAccount(List<FeedRecord> chunk) {
        LinkedHashMap<String, List<FeedRecord>> grouped = new LinkedHashMap<>();
        for (FeedRecord r : chunk) {
            grouped.computeIfAbsent(r.getAccountId(), k -> new ArrayList<>()).add(r);
        }
        return grouped;
    }

    private void processRecord(FeedRecord r) {
        switch (r.getOperation()) {
            case INSERT -> tryInsertAccount(r);
            case UPDATE_PHONE -> updatePhone(r);
            case UPDATE_NAME -> updateName(r);
            case DELETE -> deleteAccount(r);
            default -> throw new IllegalArgumentException("Unknown operation: " + r.getOperation());
        }
    }

    private void tryInsertAccount(FeedRecord r) {
        String sql = "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(account_table, account_id) */ INTO account_table (account_id, name, phone) VALUES (?, ?, ?)";
        jdbcTemplate.update(sql, r.getAccountId(), r.getName(), r.getPhone());
    }

    private void updatePhone(FeedRecord r) {
        String sql = "UPDATE account_table SET phone = ? WHERE account_id = ?";
        int rows = jdbcTemplate.update(sql, r.getPhone(), r.getAccountId());
        if (rows == 0) logMissingRecord(r, "Phone Update");
    }

    private void updateName(FeedRecord r) {
        String sql = "UPDATE account_table SET name = ? WHERE account_id = ?";
        int rows = jdbcTemplate.update(sql, r.getName(), r.getAccountId());
        if (rows == 0) logMissingRecord(r, "Name Update");
    }

    private void deleteAccount(FeedRecord r) {
        String sql = "DELETE FROM account_table WHERE account_id = ?";
        int rows = jdbcTemplate.update(sql, r.getAccountId());
        if (rows == 0) logMissingRecord(r, "Delete");
    }

    private void logDuplicate(FeedRecord r) {
        System.out.println("[DUPLICATE] " + r);
    }

    private void logMissingRecord(FeedRecord r, String action) {
        System.out.println("[MISSING] " + action + " for: " + r);
    }

    private void logError(FeedRecord r, Exception e) {
        System.err.println("[ERROR] Record: " + r + ", Error: " + e.getMessage());
    }

    private List<List<FeedRecord>> partition(List<FeedRecord> records, int size) {
        List<List<FeedRecord>> partitions = new ArrayList<>();
        for (int i = 0; i < records.size(); i += size) {
            partitions.add(records.subList(i, Math.min(i + size, records.size())));
        }
        return partitions;
    }

    public enum OperationType {
        INSERT, UPDATE_PHONE, UPDATE_NAME, DELETE
    }

    public static class FeedRecord {
        private final String accountId;
        private final String name;
        private final String phone;
        private final OperationType operation;

        // constructor, getters
        public FeedRecord(String accountId, String name, String phone, OperationType operation) {
            this.accountId = accountId;
            this.name = name;
            this.phone = phone;
            this.operation = operation;
        }

        public String getAccountId() { return accountId; }
        public String getName() { return name; }
        public String getPhone() { return phone; }
        public OperationType getOperation() { return operation; }

        @Override
        public String toString() {
            return String.format("FeedRecord[accountId=%s, name=%s, phone=%s, operation=%s]", accountId, name, phone, operation);
        }
    }
}
