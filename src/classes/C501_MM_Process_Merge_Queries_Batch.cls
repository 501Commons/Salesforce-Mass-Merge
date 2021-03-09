// Run Code FROM Developer Console
//  1) Debug -> Open Execute Anonymous Window
//  2) paste code
/*
        String strSoql = C501_MassMerge_SharedCode.GetMergeQueries();
        C501_MM_Process_Merge_Queries_Batch batchProcessMergeQueries = new C501_MM_Process_Merge_Queries_Batch();
        batchProcessMergeQueries.query = strSoql;
        batchProcessMergeQueries.start = Datetime.now();
        Database.executeBatch(batchProcessMergeQueries);
*/
//  3) click Execute

public class C501_MM_Process_Merge_Queries_Batch implements Database.Batchable<sObject>, Database.AllowsCallouts, Database.Stateful { 

    public String query;
    public Datetime start;

    public Database.querylocator start(Database.BatchableContext BC) {
        
        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Process_Merge_Queries_Batch::start query: ' + this.query);
        return Database.getQueryLocator(this.query);
    }

    public void execute(Database.BatchableContext info, List<sObject> scope) {

        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Process_Merge_Queries_Batch::execute - scope: ' + scope);

        C501_MM_Process_Merge_Queries.get().ProcessMergeMetrics(this.query);
    }

    public void finish(Database.BatchableContext info) {

        // Seconds = Convert from Millisconds / 1000
        Long elapseSeconds = (Datetime.now().getTime() - this.start.getTime()) / 1000;
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_Accounts_Batch::finish - elapseSeconds: ' + elapseSeconds);

        // Check to make sure there is still data to process
        if (C501_MassMerge_SharedCode.GetCountMergeQueries(null) <= 0 || elapseSeconds > (60 * 20) || Test.isRunningTest()) {

            String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
            if (String.isBlank(strSoql)) {
                return;
            }
                    
            Rule_Merge_Batch batchMerge = new Rule_Merge_Batch();
            batchMerge.query = strSoql;
            batchMerge.start = Datetime.now();
            batchMerge.mergeCandidates = C501_MassMerge_SharedCode.GetCountMergeCandidates();
            batchMerge.mergeCandidateRetries = 0;
         
            if (!Test.isRunningTest()) {
                Database.executeBatch(batchMerge);
            }
    
            return;
        }

        C501_MM_Process_Merge_Queries_Batch batchMerge = new C501_MM_Process_Merge_Queries_Batch();
        batchMerge.query = this.query;
        batchMerge.start = this.start;

        if (!Test.isRunningTest()) {
            Database.executeBatch(batchMerge);
        }
    }
}