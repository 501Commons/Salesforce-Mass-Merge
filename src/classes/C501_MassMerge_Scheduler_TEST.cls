/*
    Copyright (c) 2019, 501Commons.org
    All rights reserved.
    
    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:
    
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of 501Commons.org nor the names of
      its contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.
 
    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
    POSSIBILITY OF SUCH DAMAGE.
*/

@istest
class C501_MassMerge_Scheduler_TEST {

    public static String CRON_EXP = '0 0 0 3 9 ? 2022';

    @isTest(SeeAllData='true')
    public static void C501_MassMerge_Scheduler_TEST_Schedule() {

        C501_MassMerge_SharedCode.TestSetupMerge();

        Test.startTest();

        // Schedule the test job
        String jobId = System.schedule('testBasicScheduledApex',
        C501_MassMerge_Scheduler_TEST.CRON_EXP, 
            new C501_MM_Rule_Discover_Accounts());

        // Get the information FROM the CronTrigger API object
        CronTrigger ct = [SELECT Id, CronExpression, TimesTriggered, 
            NextFireTime
            FROM CronTrigger WHERE id = :jobId];

        // Verify the expressions are the same
        System.assertEquals(C501_MassMerge_Scheduler_TEST.CRON_EXP, 
            ct.CronExpression);

        // Verify the job has not run
        System.assertEquals(0, ct.TimesTriggered);

        // Verify the next time the job will run
        System.assertEquals('2022-09-03 00:00:00', 
            String.valueOf(ct.NextFireTime));

        Test.stopTest();

        String strSoql = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        strSoql += ' AND Merge_Metric__r.Name LIKE \'TESTDATA%\'';
        strSoql += ' LIMIT 1';

        //System.assert(
        //    100.0 == Database.query(strSoql).Merge_Confidence__c);
    }

    @isTest(SeeAllData='true')
    public static void C501_MassMerge_Batch_DiscoverAccount_Test() {

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return;
        }

        Rule_Discover_Accounts_Batch batchDiscover = new Rule_Discover_Accounts_Batch();
        batchDiscover.query = strSoql;
        batchDiscover.start = Datetime.now();
    
        Test.startTest();

        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Account Test');
        
        Database.executeBatch(batchDiscover);

        Test.stopTest();
    }   

    @isTest(SeeAllData='true')
    public static void C501_MassMerge_Batch_DiscoverContactsAccount_Test() {

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return;
        }

        Rule_Discover_ContactsAccount_Batch batchDiscover = new Rule_Discover_ContactsAccount_Batch();
        batchDiscover.query = strSoql;
        batchDiscover.start = Datetime.now();
    
        Test.startTest();

        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');
        
        Database.executeBatch(batchDiscover);

        Test.stopTest();
    }   

    @isTest(SeeAllData='true')
    public static void C501_MassMerge_Batch_DiscoverContactsAccounts_Test() {

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return;
        }

        Rule_Discover_ContactsAccounts_Batch batchDiscover = new Rule_Discover_ContactsAccounts_Batch();
        batchDiscover.query = strSoql;
        batchDiscover.start = Datetime.now();

        Test.startTest();
        
        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        Database.executeBatch(batchDiscover);

        Test.stopTest();
    }   
    
    @isTest(SeeAllData='true')
    public static void C501_MassMerge_Batch_MergeQueries_Test() {

        String strSoql = C501_MassMerge_SharedCode.GetMergeQueries();
        if (String.isBlank(strSoql)) {
            return;
        }

        C501_MM_Process_Merge_Queries_Batch batchMergeQueries = new C501_MM_Process_Merge_Queries_Batch();
        batchMergeQueries.query = strSoql;
        batchMergeQueries.start = Datetime.now();

        Test.startTest();
        
        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        Database.executeBatch(batchMergeQueries);

        Test.stopTest();
    }   
    
    @isTest(SeeAllData='true')
    public static void C501_MassMerge_Batch_Merge_Test() {

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return;
        }

        Rule_Merge_Batch batchMerge = new Rule_Merge_Batch();
        batchMerge.query = strSoql;
        batchMerge.start = Datetime.now();
        batchMerge.mergeCandidates = C501_MassMerge_SharedCode.GetCountMergeCandidates();

        Test.startTest();
        
        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        Database.executeBatch(batchMerge);

        Test.stopTest();
    }   
}