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

//
// Run Code FROM Developer Console
//  1) Debug -> Open Execute Anonymous Window
//  2) paste code
/*
      C501_MM_Rule_Discover.get().DiscoverGeneralProcessing();
*/
//  3) click Execute
//
// Abort scheduled jobs
/*
    List<CronJobDetail> scheduledJobs = [SELECT Id FROM CronJobDetail WHERE Name like '%C501_%'];
    for (CronJobDetail scheduledJob :scheduledJobs) {
        Id jobId = [SELECT Id FROM CronTrigger WHERE CronJobDetailId = :scheduledJob.Id].Id;
        System.abortJob(jobId);
    }
*/

public class C501_MM_Rule_Discover {

    public enum MergeCriteria { Merge_Criteria, All_Contacts_Match }

	// only need one instance of this class per transaction
	private static C501_MM_Rule_Discover instance;
	public static C501_MM_Rule_Discover get() {

		if (instance == null) instance = new C501_MM_Rule_Discover();
		return instance;
	}

    public void C501_MM_Rule_Discover() {
    }

    public void DiscoverGeneralProcessing() {

        C501_MassMerge_SharedCode.InitializeMassMergeInstances();

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        this.ProcessMergeMetrics(strSoql);
    }

    public void ProcessMergeMetrics(String strSoql) {

 		//
		// Perform all SOQL Queries outside of loop to avoid Salesforce Governor Limits: https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/apex_gov_limits.htm
		//

        strSoql += ' ORDER BY Last_Discover_Date__c ASC NULLS FIRST';
        strSoql += ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumLimitSize(1));

        System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics strSoql: ' + strSoql);

        List<C501_Merge_Metric__c> mergeMetrics = Database.Query(strSoql);

        Set<String> sourceReportIds = new Set<String>();
        for (C501_Merge_Metric__c mergeMetric : mergeMetrics) {
            if (!String.isBlank(mergeMetric.Source_Report__c)) {
                sourceReportIds.add(mergeMetric.Source_Report__c);
            }
        }

        List<Report> reports = [
            SELECT Id, DeveloperName, FolderName, LastModifiedDate
            FROM Report
            WHERE Id in :sourceReportIds OR DeveloperName in :sourceReportIds];

        // Walk through Merge Metrics
        List<C501_Merge_Metric__c> mergeMetricsUpdate = new List<C501_Merge_Metric__c>();
        Set<Id> mergeMetricDeleteChildren = new Set<Id>();

        // Delete all merge candidates where the linked report has been updated
        //
        for (C501_Merge_Metric__c mergeMetric : mergeMetrics) {

            String reportId;
            for (Report report :reports) {

                if ((String)report.Id == mergeMetric.Source_Report__c || report.DeveloperName == mergeMetric.Source_Report__c) {

                    // Check for report change
                    if (mergeMetric.Source_Report_LastModifiedDate__c == null || mergeMetric.Source_Report_LastModifiedDate__c <> report.LastModifiedDate) {

                        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:execute - Merge Metric linked Report changed so clear out all merge candidates: ' + String.valueOf(mergeMetric));
                        mergeMetric.Source_Report_LastModifiedDate__c = report.LastModifiedDate;
                        mergeMetric.Unique_Id__c = report.Id;
                        mergeMetricsUpdate.add(mergeMetric);

                        // Queue merge metric to delete children since report changed
                        mergeMetricDeleteChildren.add(mergeMetric.Id);
                    }

                    reportId = (String)report.Id;

                    break;
                }
            }
        }

        if (!mergeMetricDeleteChildren.isEmpty()) {

            String strSoqlDeleteAccountMerges = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();

            strSoqlDeleteAccountMerges += ' AND Merge_Metric__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(mergeMetricDeleteChildren);

            System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics strSoql: ' + strSoqlDeleteAccountMerges);

            List<C501_Account_Merge__c> deleteAccountMerges = Database.query(strSoqlDeleteAccountMerges);
            if (!deleteAccountMerges.isEmpty()) {
                Database.delete(deleteAccountMerges, false);
            }

            String strSoqlDeleteContactMerges = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();

            strSoqlDeleteContactMerges += ' AND Merge_Metric__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(mergeMetricDeleteChildren);

            System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics strSoql: ' + strSoqlDeleteContactMerges);

            List<C501_Contact_Merge__c> deleteContactMerges = Database.query(strSoqlDeleteContactMerges);
            if (!deleteContactMerges.isEmpty()) {

                System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics Delete contactMerges: ' + deleteContactMerges);
                Database.delete(deleteContactMerges, false);
            }
        }

        if (!mergeMetricsUpdate.isEmpty()) {
            Database.SaveResult[] updateResults = Database.update(mergeMetricsUpdate, false);
            for (Database.SaveResult updateResult :updateResults) {
                if (!updateResult.isSuccess()) {
                    for(Database.Error resultError : updateResult.getErrors()) {
                        C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics');
                    }                        
                }
            }
        }
    }

    public void ProcessReport(String reportId, C501_Merge_Metric__c mergeMetric, String processObjectType) {

        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport reportId: ' + reportId + ' mergeMetric: ' + String.valueOf(mergeMetric));

        // Get the report metadata
        Reports.ReportMetadata reportMetadata = Reports.ReportManager.describeReport(reportId).getReportMetadata();

        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport Name: ' + reportMetadata.getName());
        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport ID: ' + reportMetadata.getId());
        if (String.isNotBlank(reportMetadata.getDescription())) {
            System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport Description: ' + reportMetadata.getDescription());
        }
        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport Developer name: ' + reportMetadata.getDeveloperName());
        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport Report format: ' + reportMetadata.getReportFormat());
        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport Report Type - Label: ' + reportMetadata.getReportType().getLabel());
        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport Report Type - Type: ' + reportMetadata.getReportType().getType());
        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport Scope: ' + reportMetadata.getScope());
        if (String.isNotBlank(reportMetadata.getReportBooleanFilter())) {
            System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport Boolean filter: ' + reportMetadata.getReportBooleanFilter());
        }
        Reports.StandardDateFilter standardDateFilter = reportMetadata.getStandardDateFilter();
        if (standardDateFilter <> null) {
            System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport Standard Filters: ' + String.valueOf(standardDateFilter));
        }

        List<String> detailColumns = reportMetadata.getDetailColumns();
        if (detailColumns.size() > 0) {
            System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport getDetailColumns: ' + String.valueOf(detailColumns));
        }
        List<Reports.ReportFilter> reportFilters = reportMetadata.getReportFilters();
        if (reportFilters.size() > 0) {
            System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport getReportFilters: ' + String.valueOf(reportFilters));
        }
        List<Reports.CrossFilter> crossFilters = reportMetadata.getCrossFilters();
        if (crossFilters.size() > 0) {
            System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport Cross filters: ' + String.valueOf(crossFilters));
        }
        List<Reports.StandardFilter> standardFilters = reportMetadata.getStandardFilters();
        if (standardFilters.size() > 0) {
            System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReport Standard Filters: ' + String.valueOf(standardFilters));
        }

        // Translate Report columns to Soql fields

        // Get FK - Foriegn Key Object
        String foriegnKey;
        List<String> foriegnKeyObjects = reportMetadata.getReportType().getType().split('\\.');
        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover::ProcessReport - foriegnKeyObjects: ' + foriegnKeyObjects);
        if (!foriegnKeyObjects.isEmpty()) {
            foriegnKey = foriegnKeyObjects[foriegnKeyObjects.size() - 1].replace('__c', '__r.');
        }

        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover::ProcessReport - foriegnKey: ' + foriegnKey);

        Boolean usingAddressColumns = false;
        List<String> detailColumnsChecked = new List<String>();
        List<String> mergeCriteriaValues = new List<String>();
        for (String column :detailColumns) {

            if (column.containsIgnoreCase('ADDRESS')) {
                usingAddressColumns = true;
            }

            String checkColumn = this.CheckReportColumnName(mergeMetric, foriegnKey, column, false);
            if (String.isNotBlank(checkColumn) && !checkColumn.containsIgnoreCase(MergeCriteria.Merge_Criteria.name())) {
                detailColumnsChecked.add(checkColumn);
            }
            else if (String.isNotBlank(checkColumn) && checkColumn.containsIgnoreCase(MergeCriteria.Merge_Criteria.name())) {
                mergeCriteriaValues.add(checkColumn);
            }
        }

        // Discover merge candidates
        this.ProcessReportDiscover(mergeMetric, foriegnKey, detailColumnsChecked, mergeCriteriaValues, reportFilters, processObjectType);

        // Check for Address columns
        //  When using address columns need to query both address, Shipping or Billing, values because regardless of
        //  which you specify in the report the column name comes in as Address1 instead of Shipping or Billing
        //  For contact just swapping Mailing and Other.
        if (usingAddressColumns) {

            List<String> detailColumnsCheckedAddressReverse = new List<String>();
            for (String column :detailColumnsChecked) {
                if (column.contains('Account.Billing')) {
                    detailColumnsCheckedAddressReverse.add(column.replace('Account.Billing', 'Account.Shipping'));
                }
                else if (column.contains('Account.Shipping')) {
                    detailColumnsCheckedAddressReverse.add(column.replace('Account.Shipping', 'Account.Billing'));
                }
                else if (column.contains('Mailing')) {
                    detailColumnsCheckedAddressReverse.add(column.replace('Mailing', 'Other'));
                }
                else if (column.contains('Other')) {
                    detailColumnsCheckedAddressReverse.add(column.replace('Other', 'Mailing'));
                }
                else {
                    detailColumnsCheckedAddressReverse.add(column);
                }
            }

            // Discover merge candidates
            this.ProcessReportDiscover(mergeMetric, foriegnKey, detailColumnsCheckedAddressReverse, mergeCriteriaValues, reportFilters, processObjectType);
        }
    }

    public void ProcessReportDiscover(C501_Merge_Metric__c mergeMetric, String foriegnKey, List<String> detailColumnsChecked, List<String> mergeCriteriaValues, List<Reports.ReportFilter> reportFilters, String processObjectType) {

        if (processObjectType.equalsIgnoreCase('Account')) {

            // Process by Accounts to find any potential Account merges
            this.ProcessReportByAccounts(mergeMetric, foriegnKey, detailColumnsChecked, mergeCriteriaValues, reportFilters);
        }
        else if (processObjectType.equalsIgnoreCase('Contacts Across Accounts')) {

            // Process by Contacts different Accounts to find any potential contact merges across different accounts
            this.ProcessReportByContacts(mergeMetric, foriegnKey, detailColumnsChecked, mergeCriteriaValues, reportFilters, null);
        }
        else if (processObjectType.equalsIgnoreCase('Contacts Within Account')) {

            // Process by Contacts same Account to find any potential contacts merges within same account
            this.ProcessReportByContacts(mergeMetric, foriegnKey, detailColumnsChecked, mergeCriteriaValues, reportFilters, 'AccountId');
        }
    }
    
    public void ProcessReportByAccounts(C501_Merge_Metric__c mergeMetric, String foriegnKey, List<String> detailColumnsChecked, List<String> mergeCriteriaValues, List<Reports.ReportFilter> reportFilters) {

        // Find Contact matches across Accounts
        //
        String strSoqlSelect = 'SELECT Count(' + mergeMetric.Source_Object_Account_Id__c + ')Matches, MAX(' + mergeMetric.Source_Object_Account_Id__c + ')MaxId, MIN(' + mergeMetric.Source_Object_Account_Id__c + ')MinId ';
        String strSoqlFrom = ' FROM ' + mergeMetric.Source_Object__c;

        // Build WHERE statement
        //
        List<String> whereFields = new List<String>();
        List<String> whereInValues = new List<String>();

        // Add Null checks
        for (String column :detailColumnsChecked) {

            // Skip Null check on fields not required to uniquely group contacts
            if (column.equalsIgnoreCase('gender__c')) {
                continue;
            }

            whereFields.add(column + ' <> null ');
        }

        // Add filters
        for (Reports.ReportFilter reportFilter :reportFilters) {

            String whereField = this.CheckReportOperator(mergeMetric, foriegnKey, reportFilter.getColumn(), reportFilter.getOperator(), reportFilter.getValue());
            if (String.isNotBlank(whereField)) {

                // Check if value contains a list of Strings
                if (reportFilter.getValue().contains(',')) {
                    whereInValues = reportFilter.getValue().split(',');

                    whereField = this.CheckReportColumnName(mergeMetric, foriegnKey, reportFilter.getColumn(), true) + ' in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(whereInValues);
                }

                whereFields.add(whereField);
            }
        }

        String strSoqlWhere = ' WHERE ' + String.join(whereFields, ' and ');
        strSoqlWhere += C501_MassMerge_SharedCode_Account.WhereFilterByMerges(mergeMetric.Source_Object_Account_Id__c);

        // Add aggregate check
        List<String> groupByFields = new List<String>();
        for (String column :detailColumnsChecked) {

            if (column.equalsIgnoreCase(mergeMetric.Source_Object_Account_Id__c)) {
                continue;
            }

            groupByFields.add(column);
        }

        // Check for valid group fields
        if (groupByFields.size() <= 0) {
            return;
        }

        String strSoqlGroupBy = ' GROUP BY ' + String.join(groupByFields, ', ');
        String strSoqlOrderBy = ' ORDER BY Count(' + mergeMetric.Source_Object_Account_Id__c + ') DESC';

        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReportByAccounts aggregate SOQL');
        System.debug(LoggingLevel.DEBUG, strSoqlSelect);
        System.debug(LoggingLevel.DEBUG, strSoqlFrom);
        System.debug(LoggingLevel.DEBUG, strSoqlWhere);
        System.debug(LoggingLevel.DEBUG, strSoqlGroupBy);
        System.debug(LoggingLevel.DEBUG, strSoqlOrderBy);

        Integer maxCountInterval = 10;
        for (Integer countIndex = 2; countIndex <= maxCountInterval; countIndex++) {

            String countInterval = (countIndex >= maxCountInterval) ? ' >= ' + String.valueOf(countIndex) : ' = ' + String.valueOf(countIndex);

            if (C501_MassMerge_SharedCode.NearSalesforceLimits()) {
                System.debug(LoggingLevel.WARN, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByAccounts Near Salesforce Limits so kick out on processing countInterval: ' + countInterval + ' of maxCountInterval: ' + String.valueOf(maxCountInterval));
                break;
            }

            String strSoqlHaving = ' HAVING Count(' + mergeMetric.Source_Object_Account_Id__c + ') ' + countInterval;
            System.debug(LoggingLevel.DEBUG, strSoqlHaving);
            String strSoqlLimit = ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumLimitSize(groupByFields.size()));
            System.debug(LoggingLevel.DEBUG, strSoqlLimit);

            AggregateResult[] allAggregatedByAccounts;
            try {
                System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByAccounts strSoql: ' + strSoqlSelect + strSoqlFrom + strSoqlWhere + strSoqlGroupBy + strSoqlHaving + strSoqlOrderBy + strSoqlLimit);
                allAggregatedByAccounts = Database.Query(strSoqlSelect + strSoqlFrom + strSoqlWhere + strSoqlGroupBy + strSoqlHaving + strSoqlOrderBy + strSoqlLimit);
            }
            catch (System.LimitException exLimit) {
                System.debug(LoggingLevel.ERROR, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByAccounts strSoql: ' + strSoqlSelect + strSoqlFrom + strSoqlWhere + strSoqlGroupBy + strSoqlHaving + strSoqlOrderBy + strSoqlLimit);
                System.debug(LoggingLevel.ERROR, '***** C501_MM_Rule_Discover:ProcessReportByAccounts SOQL System.LimitException Error: ' + exLimit.getMessage());
                return;
            }
            catch (Exception ex) {
                System.debug(LoggingLevel.ERROR, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByAccounts strSoql: ' + strSoqlSelect + strSoqlFrom + strSoqlWhere + strSoqlGroupBy + strSoqlHaving + strSoqlOrderBy + strSoqlLimit);
                System.debug(LoggingLevel.ERROR, '***** C501_MM_Rule_Discover:ProcessReportByAccounts SOQL Error: ' + ex.getMessage());
                return;
            }

            if (allAggregatedByAccounts.isEmpty()) {
                continue;
            }

            // Filter out account ids already associated with merge metric
            Set<Id> existingAccountIds = new Set<Id>();

            String strSoql = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
            strSoql += ' AND Merge_Metric__c = \'' + mergeMetric.Id + '\'';

            System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByAccounts strSoql: ' + strSoql);

            for(C501_Account_Merge__c existingMerge : Database.query(strSoql)) {
                    existingAccountIds.add(existingMerge.Account__c);
                    existingAccountIds.add(existingMerge.Merge_Target_Account__c);
            }

            // Check for additinal Merge Criteria
            //
            Map<Id,String> accountContacts = new Map<Id,String>();
            if (this.ListContainsValue(mergeCriteriaValues, MergeCriteria.All_Contacts_Match.name())) {
                // Schedule All Contacts Match Processing
//                C501_MassMerge_Fx_Base fxBase = new C501_MassMerge_Fx_AllContactsMatch();

                Set<Id> mergeAccountIds = new Set<Id>();
                String minId, maxId;
                for (AggregateResult result : allAggregatedByAccounts)  {
                    
                    maxId = result.get('MaxId').toString();
                    minId = result.get('MinId').toString();

                    if (String.isBlank(maxId) || String.isBlank(minId) || maxId == minId) {
                        continue;
                    }

                    if (existingAccountIds.contains(maxId) || existingAccountIds.contains(minId)) {
                        continue;
                    }

                    mergeAccountIds.add(maxId);
                    mergeAccountIds.add(minId);
                }

                // Get Household Sizes
                Map<Id, Integer> existingAccountSizes = new Map<Id, Integer>();

                strSoql = 'SELECT AccountId, Count(Id) NumberOfMembers FROM Contact WHERE AccountId IN ' + C501_MassMerge_SharedCode.GetSafeSoqlList(mergeAccountIds);
                strSoql += ' group by AccountId HAVING Count(Id) > 0 LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumLimitSize(1));

                System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByAccounts strSoql: ' + strSoql);

                for (AggregateResult result : Database.query(strSoql)) {

                    String accountId = result.get('AccountId').toString();
                    Integer numberOfMembers = Integer.valueOf(result.get('NumberOfMembers'));
                    existingAccountSizes.put(accountId, numberOfMembers);
                }

                Set<Id> mergeAccountIdsMatching = new Set<Id>();
                for (AggregateResult result : allAggregatedByAccounts)  {

                    maxId = result.get('MaxId').toString();
                    minId = result.get('MinId').toString();

                    if (!existingAccountSizes.containsKey(maxId) || !existingAccountSizes.containsKey(minId)) {
                        continue;
                    }

                    // Make sure household contact count equal
                    if (existingAccountSizes.get(maxId) <> existingAccountSizes.get(minId)) {
                        continue;
                    }

                    mergeAccountIdsMatching.add(maxId);
                    mergeAccountIdsMatching.add(minId);
                }

                strSoql = 'SELECT AccountId, FirstName, LastName, Birthdate FROM Contact WHERE AccountId IN ' + C501_MassMerge_SharedCode.GetSafeSoqlList(mergeAccountIdsMatching);
                strSoql += ' ORDER BY AccountId, FirstName, LastName, Birthdate DESC LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumLimitSize(1));

                System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByAccounts strSoql: ' + strSoql);

                for (Contact contactInfo :Database.query(strSoql)) {
                    String contactsInfo;
                    if (accountContacts.containsKey(contactInfo.AccountId)) {
                        contactsInfo = accountContacts.remove(contactInfo.AccountId);
                    }

                    contactsInfo += 
                        (String.isNotBlank(contactInfo.FirstName) ? contactInfo.FirstName.mid(1,1) : '')
                        + (String.isNotBlank(contactInfo.LastName) ? contactInfo.LastName : '')
                        + (contactInfo.Birthdate <> null ? String.valueOf(contactInfo.Birthdate) : '');

                    accountContacts.put(contactInfo.AccountId, contactsInfo);
                }
            }

            List<C501_Account_Merge_Member__c> insertMergeMembers = new List<C501_Account_Merge_Member__c>();
            Map<String, C501_Account_Merge__c> mergeAccounts = new Map<String, C501_Account_Merge__c>();
            String uniqueId, minId, maxId;
            for (AggregateResult result : allAggregatedByAccounts)  {
                maxId = result.get('MaxId').toString();
                minId = result.get('MinId').toString();

                if (String.isBlank(maxId) || String.isBlank(minId) || maxId == minId) {
                    continue;
                }

                // Check if merge metric already created
                if (existingAccountIds.contains(maxId) || existingAccountIds.contains(minId)) {
                    continue;
                }

                // Check if All Contacts Match meaning every contact in the households has to match
                if (this.ListContainsValue(mergeCriteriaValues, MergeCriteria.All_Contacts_Match.name())) {
                    if (!accountContacts.containsKey(maxId) || !accountContacts.containsKey(minId)) {
                        continue;
                    }

                    // Validate Contacts same between both accounts
                    if (accountContacts.get(maxId) <> accountContacts.get(minId)) {
                        continue;
                    }
                }

                uniqueId = mergeMetric.Name + maxId + minId;
                mergeAccounts.put(uniqueId, new C501_Account_Merge__c(
                    Unique_Id__c = uniqueId,
                    Merge_Confidence__c = mergeMetric.Confidence__c,
                    Account__c = minId,
                    Merge_Target_Account__c = maxId,
                    Merge_Metric__c = mergeMetric.Id
                ));


                Integer matches = Integer.valueOf(result.get('Matches'));

                // Monitor matches greater than 2 because that means there are possibly some accounts between min/max that are not getting included in a merge candidate
                // Investigate doing some post processing of the accounts between min/max to see if they can be included in merge candidates
                if (matches > 2) {
                    insertMergeMembers.add(
                        new C501_Account_Merge_Member__c(
                            Account__c = minId,
                            Merge_Metric__c = mergeMetric.Id,
                            Unique_Id__c = mergeMetric.Id + minId,
                            Potential_Merges__c = matches
                        ));

                    insertMergeMembers.add(
                        new C501_Account_Merge_Member__c(
                            Account__c = maxId,
                            Merge_Metric__c = mergeMetric.Id,
                            Unique_Id__c = mergeMetric.Id + maxId,
                            Potential_Merges__c = matches
                        ));
                }
            }

            if (!insertMergeMembers.isEmpty()) {
            
               // Insert any merge members that are not in Saleforce - saves a query to just let the DB either update or insert records
                //
                Database.SaveResult[] insertResults = Database.insert(insertMergeMembers, false);
                for (Database.SaveResult result : insertResults) {
                    if (!result.isSuccess()) {
                        for(Database.Error resultError : result.getErrors()) {
                            C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByAccounts');
                        }                        
                    }
                }
            }

            if (!mergeAccounts.isEmpty()) {
                C501_MassMerge_SharedCode_Account c501MassMerge_SharedCode_Account = new C501_MassMerge_SharedCode_Account();
                c501MassMerge_SharedCode_Account.InsertObjectMergeCandidates(mergeMetric, mergeAccounts.values());
            }
        }     
    }

    public void ProcessReportByContacts(C501_Merge_Metric__c mergeMetric, String foriegnKey, List<String> detailColumnsChecked, List<String> mergeCriteriaValues, List<Reports.ReportFilter> reportFilters, String additionalGroupByField) {

        // Only process Contact from Contact Source_Object queries (e.g., From Contact in a SOQL Query)
        if (mergeMetric.Source_Object__c <> 'Contact') {
            return;
        }

        // Find Contact matches across Accounts
        //
        String strSoqlSelect = 'SELECT Count(' + mergeMetric.Source_Object_Contact_Id__c + ')Matches, MAX(' + mergeMetric.Source_Object_Contact_Id__c + ')MaxId, MIN(' + mergeMetric.Source_Object_Contact_Id__c + ')MinId ';
        String strSoqlFrom = ' FROM ' + mergeMetric.Source_Object__c;

        // Build WHERE statement
        //
        List<String> whereFields = new List<String>();
        List<String> whereInValues = new List<String>();
 
        // Add Null checks
        for (String column :detailColumnsChecked) {

            // Skip Null check on fields not required to uniquely group contacts
            if (column.equalsIgnoreCase('gender__c')) {
                continue;
            }

            whereFields.add(column + ' <> null ');
        }

        // Add filters
        for (Reports.ReportFilter reportFilter :reportFilters) {
            String whereField = this.CheckReportOperator(mergeMetric, foriegnKey, reportFilter.getColumn(), reportFilter.getOperator(), reportFilter.getValue());

            if (!String.isBlank(whereField)) {

                System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByContacts whereField: ' + whereField);

                // Check if value contains a list of Strings
                if (reportFilter.getValue().contains(',')) {
                    whereInValues = reportFilter.getValue().split(',');

                    whereField = this.CheckReportColumnName(mergeMetric, foriegnKey, reportFilter.getColumn(), true) + ' in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(whereInValues);
                }

                System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByContacts whereField: ' + whereField);

                whereFields.add(whereField);
            }
        }

        String strSoqlWhere = ' WHERE ' + String.join(whereFields, ' and ');
        strSoqlWhere += C501_MassMerge_SharedCode_Contact.WhereFilterByMerges(mergeMetric.Source_Object_Contact_Id__c);

        // Add aggregate check
        List<String> groupByFields = new List<String>();
        for (String column :detailColumnsChecked) {

            if (column.equalsIgnoreCase(mergeMetric.Source_Object_Contact_Id__c)) {
                continue;
            }

            groupByFields.add(column);
        }

        // Check to add additional groupByFields
        if (String.isNotBlank(additionalGroupByField)) {
            groupByFields.add(additionalGroupByField);
        }

        String strSoqlGroupBy = ' GROUP BY ' + String.join(groupByFields, ', ');
        String strSoqlOrderBy = ' ORDER BY Count(' + mergeMetric.Source_Object_Contact_Id__c + ') DESC';

        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Discover:ProcessReportByContacts aggregate SOQL');
        System.debug(LoggingLevel.DEBUG, strSoqlSelect);
        System.debug(LoggingLevel.DEBUG, strSoqlFrom);
        System.debug(LoggingLevel.DEBUG, strSoqlWhere);
        System.debug(LoggingLevel.DEBUG, strSoqlGroupBy);
        System.debug(LoggingLevel.DEBUG, strSoqlOrderBy);

        Integer maxCountInterval = 10;
        for (Integer countIndex = 2; countIndex <= maxCountInterval; countIndex++) {

            String countInterval = (countIndex >= maxCountInterval) ? ' >= ' + String.valueOf(countIndex) : ' = ' + String.valueOf(countIndex);

            if (C501_MassMerge_SharedCode.NearSalesforceLimits()) {
                System.debug(LoggingLevel.WARN, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByContacts Near Salesforce Limits so kick out on processing countInterval: ' + countInterval + ' of maxCountInterval: ' + String.valueOf(maxCountInterval));
                break;
            }

            String strSoqlHaving = ' HAVING Count(' + mergeMetric.Source_Object_Contact_Id__c + ') ' + countInterval;
            System.debug(LoggingLevel.DEBUG, strSoqlHaving);
            String strSoqlLimit = ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumLimitSize(groupByFields.size()));
            System.debug(LoggingLevel.DEBUG, strSoqlLimit);

            AggregateResult[] allAggregatedByContacts;
            try {
                System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByContacts strSoql: ' + strSoqlSelect + strSoqlFrom + strSoqlWhere + strSoqlGroupBy + strSoqlHaving + strSoqlOrderBy + strSoqlLimit);
                allAggregatedByContacts = Database.Query(strSoqlSelect + strSoqlFrom + strSoqlWhere + strSoqlGroupBy + strSoqlHaving + strSoqlOrderBy + strSoqlLimit);
            }
            catch (System.LimitException exLimit) {
                System.debug(LoggingLevel.ERROR, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByContacts strSoql: ' + strSoqlSelect + strSoqlFrom + strSoqlWhere + strSoqlGroupBy + strSoqlHaving + strSoqlOrderBy + strSoqlLimit);
                System.debug(LoggingLevel.ERROR, '***** C501_MM_Rule_Discover:ProcessReportByContacts SOQL System.LimitException Error: ' + exLimit.getMessage());
                return;
            }
            catch (Exception ex) {
                System.debug(LoggingLevel.ERROR, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessReportByContacts strSoql: ' + strSoqlSelect + strSoqlFrom + strSoqlWhere + strSoqlGroupBy + strSoqlHaving + strSoqlOrderBy + strSoqlLimit);
                System.debug(LoggingLevel.ERROR, '***** C501_MM_Rule_Discover:ProcessReportByContacts SOQL Error: ' + ex.getMessage());
                return;
            }

            if (allAggregatedByContacts.isEmpty()) {
                continue;
            }

            Map<String, C501_Contact_Merge__c> mergeContacts = new Map<String, C501_Contact_Merge__c>();
            String uniqueId, minId, maxId;
            for (AggregateResult result : allAggregatedByContacts)  {
                maxId = result.get('MaxId').toString();
                minId = result.get('MinId').toString();

                if (String.isBlank(maxId) || String.isBlank(minId) || maxId == minId) {
                    continue;
                }

                uniqueId = mergeMetric.Name + maxId + minId;
                mergeContacts.put(uniqueId, new C501_Contact_Merge__c(
                    Unique_Id__c = uniqueId,
                    Merge_Confidence__c = mergeMetric.Confidence__c,
                    Contact__c = minId,
                    Merge_Target_Contact__c = maxId,
                    Merge_Metric__c = mergeMetric.Id
                ));

                // Check to schedule additional processing if Matches > 2
                Integer matches = Integer.valueOf(result.get('Matches'));
                if (matches > 2) {
                    // TBG - batch schedule - figure out how to get through all even if more than 100 by using a custom checked date or something to send some each time Discover call
                    //  Process - attempt to match every 2 Ids and if Odd size then at the end match the first and last together.
                }
            }

            if (!mergeContacts.isEmpty()) {
                C501_MassMerge_SharedCode_Contact c501MassMerge_SharedCode_Contact = new C501_MassMerge_SharedCode_Contact();
                c501MassMerge_SharedCode_Contact.InsertObjectMergeCandidates(mergeMetric, mergeContacts.values());
            }
        }
    }

    // Report Column Field Names don't always match the Object Field Name
    public String CheckReportColumnName(C501_Merge_Metric__c mergeMetric, String foriegnKey, String columnFieldName, Boolean filterColumn) {

        // Filter columns - some columns not supported except in filters (i.e., Group )
        //
        if (!filterColumn) {
            if (columnFieldName.containsIgnoreCase('npsp__')) {
                return null;
            }
            else if (columnFieldName.equalsIgnoreCase('LAST_UPDATE')) {
                return null;
            }
            else if (columnFieldName.equalsIgnoreCase('CREATED_DATE')) {
                return null;
            }
            else if (columnFieldName.equalsIgnoreCase('ACCOUNT_ID')) {
                if (mergeMetric.Source_Object__c.equalsIgnoreCase('Account')) {
                    return null;
                }
                else {
                    return 'AccountId';
                }
            }
        }

        // Check if Source Object in the Column Name
        if (columnFieldName.startsWithIgnoreCase(mergeMetric.Source_Object__c + '.')) {
            columnFieldName = columnFieldName.remove(mergeMetric.Source_Object__c + '.');
        }

        // Check for FK - Foriegn Key
        if (columnFieldName.startsWithIgnoreCase('FK_')) {
            List<String> foriegnKeyFields = columnFieldName.split('\\.');
            if (foriegnKeyFields.size() == 2) {
                return foriegnKey + foriegnKeyFields[1];
            }

            return null;
        }

        // Check to add RecordTypeId
        if (columnFieldName.endsWithIgnoreCase('_RECORDTYPE')) {

            String fieldName = 'RecordTypeId';
            if (mergeMetric.Source_Object__c.equalsIgnoreCase('Account')) {

                Map<String, Schema.SObjectField> objectFields = Schema.SObjectType.Account.fields.getMap();
                if (objectFields.keySet().contains(fieldName.toLowerCase())) {
                    Schema.SObjectField f = objectFields.get(fieldName);
                    Schema.DescribeFieldResult fResult = f.getDescribe();

                    if (fResult.isAccessible()) {
                        if (columnFieldName.containsIgnoreCase('Account')) {
                            return 'Account.RecordType.Name';
                        }
                    }
                }
            }
            else if (mergeMetric.Source_Object__c.equalsIgnoreCase('Contact')) {

                Map<String, Schema.SObjectField> objectFields = Schema.SObjectType.Contact.fields.getMap();
                if (objectFields.keySet().contains(fieldName.toLowerCase())) {
                    Schema.SObjectField f = objectFields.get(fieldName);
                    Schema.DescribeFieldResult fResult = f.getDescribe();

                    if (fResult.isAccessible()) {
                        if (columnFieldName.containsIgnoreCase('Contact')) {
                            return 'Contact.RecordType.Name';
                        }
                    }
                }
            }
        }

        // Contact General Columns
        //
        else if (columnFieldName.equalsIgnoreCase('NAME')) {
            return 'Name';
        }
        else if (columnFieldName.equalsIgnoreCase('EMAIL')) {
            return 'Email';
        }
        else if (columnFieldName.equalsIgnoreCase('FIRST_NAME')) {
            return 'FirstName';
        }
        else if (columnFieldName.equalsIgnoreCase('LAST_NAME')) {
            return 'LastName';
        }
        else if (columnFieldName.equalsIgnoreCase('MIDDLE_NAME')) {
            return 'MiddleName';
        }
        else if (columnFieldName.equalsIgnoreCase('BIRTHDATE')) {
            return 'Birthdate';
        }
        else if (columnFieldName.equalsIgnoreCase('REPORTS_TO')) {
            return 'ReportsTo.Name';
        }
        else if (columnFieldName.equalsIgnoreCase('CONTACT_LAST_UPDATE_BY_ALIAS')) {
            return 'LastModifiedBy.Alias';
        }
        else if (columnFieldName.equalsIgnoreCase('CONTACT_LAST_UPDATE_BY')) {
            return 'LastModifiedBy.Name';
        }
        else if (columnFieldName.equalsIgnoreCase('CONTACT_CREATED_ALIAS')) {
            return 'CreatedBy.Alias';
        }
        else if (columnFieldName.equalsIgnoreCase('CONTACT_CREATED')) {
            return 'CreatedBy.Name';
        }
        else if (columnFieldName.equalsIgnoreCase('CONTACT_ALIAS')) {
            return 'Owner.Alias';
        }
        else if (columnFieldName.equalsIgnoreCase('CONTACT_OWNER')) {
            return 'Owner.Name';
        }

        // Contact Address Columns
        //
        else if (columnFieldName.startsWithIgnoreCase('ADDRESS1_LINE')) {
            return null;
        }
        else if (columnFieldName.startsWithIgnoreCase('ADDRESS2_LINE')) {
            return null;
        }
        else if (columnFieldName.startsWithIgnoreCase('ADDRESS1_ZIP')) {
            return 'MailingPostalCode';
        }
        else if (columnFieldName.startsWithIgnoreCase('ADDRESS2_ZIP')) {
            return 'OtherPostalCode';
        }
        else if (columnFieldName.startsWithIgnoreCase('ADDRESS1_STATE')) {
            return 'MailingState';
        }
        else if (columnFieldName.startsWithIgnoreCase('ADDRESS2_STATE')) {
            return 'OtherState';
        }
        else if (columnFieldName.startsWithIgnoreCase('ADDRESS1_CITY')) {
            return 'MailingCity';
        }
        else if (columnFieldName.startsWithIgnoreCase('ADDRESS2_CITY')) {
            return 'OtherCity';
        }
        else if (columnFieldName.startsWithIgnoreCase('ADDRESS1_STREET')) {
            return 'MailingStreet';
        }
        else if (columnFieldName.startsWithIgnoreCase('ADDRESS2_STREET')) {
            return 'OtherStreet';
        }
        else if (columnFieldName.startsWithIgnoreCase('ADDRESS1_')) {
            return columnFieldName.replace('ADDRESS1_', 'Mailing');
        }
        else if (columnFieldName.startsWithIgnoreCase('ADDRESS2_')) {
            return columnFieldName.replace('ADDRESS2_', 'Other');
        }

        // Contact Other Columns
        //
        else if (columnFieldName.equalsIgnoreCase('IS_EMAIL_BOUNCED')) {
            return 'IsEmailBounced';
        }
        else if (columnFieldName.equalsIgnoreCase('FAX_OPT_OUT')) {
            return 'HasOptedOutOfFax';
        }
        else if (columnFieldName.equalsIgnoreCase('EMAIL_OPT_OUT')) {
            return 'HasOptedOutOfEmail';
        }
        else if (columnFieldName.equalsIgnoreCase('PHONE1')) {
            return 'Phone';
        }
        else if (columnFieldName.startsWithIgnoreCase('ASSISTANT_NAME')) {
            return 'AssistantName';
        }
        else if (columnFieldName.startsWithIgnoreCase('LEAD_SOURCE')) {
            return 'LeadSource';
        }

        // Account General Columns
        //
        else if (columnFieldName.equalsIgnoreCase('ACCOUNT_ID')) {
            if (mergeMetric.Source_Object__c.equalsIgnoreCase('Contact')) {
                return 'AccountId';
            }
        }
        else if (columnFieldName.equalsIgnoreCase('ACCOUNT.NAME')) {
            return 'Account.Name';
        }
        else if (columnFieldName.equalsIgnoreCase('ACCOUNT_NUMBER')) {
            return 'Account.AccountNumber';
        }
        else if (columnFieldName.equalsIgnoreCase('ACCOUNT_SOURCE')) {
            return 'Account.AccountSource';
        }
        else if (columnFieldName.equalsIgnoreCase('SIC_DESC')) {
            return 'Account.SicDesc';
        }        
        else if (columnFieldName.equalsIgnoreCase('TICKER')) {
            return 'Account.TickerSymbol';
        }        
        else if (columnFieldName.equalsIgnoreCase('PARENT_ID')) {
            return 'Account.ParentId';
        }
        else if (columnFieldName.equalsIgnoreCase('OWNER_FULL_NAME')) {
            return 'Account.Owner.Name';
        }
        else if (columnFieldName.equalsIgnoreCase('ALIAS')) {
            return 'Account.Owner.Alias';
        }

        // Account Address Columns
        //
        else if (columnFieldName.startsWithIgnoreCase('ACCOUNT.ADDRESS1_LINE')) {
            return null;
        }
        else if (columnFieldName.startsWithIgnoreCase('ACCOUNT.ADDRESS2_LINE')) {
            return null;
        }
        else if (columnFieldName.startsWithIgnoreCase('ACCOUNT.ADDRESS1_ZIP')) {
            return 'Account.BillingPostalCode';
        }
        else if (columnFieldName.startsWithIgnoreCase('ACCOUNT.ADDRESS2_ZIP')) {
            return 'Account.ShippingPostalCode';
        }
        else if (columnFieldName.startsWithIgnoreCase('ACCOUNT.ADDRESS1_STATE')) {
            return 'Account.BillingState';
        }
        else if (columnFieldName.startsWithIgnoreCase('ACCOUNT.ADDRESS2_STATE')) {
            return 'Account.ShippingState';
        }
        else if (columnFieldName.startsWithIgnoreCase('ACCOUNT.ADDRESS1_CITY')) {
            return 'Account.BillingCity';
        }
        else if (columnFieldName.startsWithIgnoreCase('ACCOUNT.ADDRESS2_CITY')) {
            return 'Account.ShippingCity';
        }
        else if (columnFieldName.startsWithIgnoreCase('ACCOUNT.ADDRESS1_STREET')) {
            return 'Account.BillingStreet';
        }
        else if (columnFieldName.startsWithIgnoreCase('ACCOUNT.ADDRESS2_STREET')) {
            return 'Account.ShippingStreet';
        }
        else if (columnFieldName.startsWithIgnoreCase('ACCOUNT.ADDRESS1_')) {
            return columnFieldName.replace('ACCOUNT.ADDRESS1_', 'Account.Billing');
        }
        else if (columnFieldName.startsWithIgnoreCase('ACCOUNT.ADDRESS2_')) {
            return columnFieldName.replace('ACCOUNT.ADDRESS2_', 'Account.Shipping');
        }

        // Account Other Columns
        //
        else if (columnFieldName.equalsIgnoreCase('URL')) {
            return 'Account.Website';
        }
        else if (columnFieldName.equalsIgnoreCase('ACCOUNT.PHONE1')) {
            return 'Account.Phone';
        }

        // Check for Source Object in Column Name
        else if (columnFieldName.startsWithIgnoreCase(mergeMetric.Source_Object__c + '.')) {
            String modifiedColumnFieldName = columnFieldName.split('\\.')[1];
            return modifiedColumnFieldName;
        }

        // Check for Custom Field
        else if (columnFieldName.endsWithIgnoreCase('__c')) {
            return columnFieldName;
        }

        return null;
    }

    public String CheckReportOperator(C501_Merge_Metric__c mergeMetric, String foriegnKey, String reportColumnName, String operator, String value) {

        String columnName = this.CheckReportColumnName(mergeMetric, foriegnKey, reportColumnName, true);
        if (String.isBlank(columnName)) {
            return null;
        }

        String checkedValue = (value.isNumeric() ? value : ('\'' + value + '\''));

        if (operator.equalsIgnoreCase('equals')) {
            return columnName + ' = ' + checkedValue;
        }
        else if (operator.equalsIgnoreCase('notEqual')) {
            return columnName + ' != ' + checkedValue;
        }
        else if (operator.equalsIgnoreCase('lessThan')) {
            return columnName + ' < ' + checkedValue;
        }
        else if (operator.equalsIgnoreCase('greaterThan')) {
            return columnName + ' > ' + checkedValue;
        }
        else if (operator.equalsIgnoreCase('lessOrEqual')) {
            return columnName + ' <= ' + checkedValue;
        }
        else if (operator.equalsIgnoreCase('greaterOrEqual')) {
            return columnName + ' >= ' + checkedValue;
        }
        else if (operator.equalsIgnoreCase('contains')) {
            return columnName + ' LIKE \'%' + value + '%\'';
        }
        else if (operator.equalsIgnoreCase('notContain')) {
            return ' (NOT ' + columnName + ' LIKE \'%' + value + '%\')';
        }
        else if (operator.equalsIgnoreCase('startsWith')) {
            return columnName + ' LIKE \'' + value + '%\'';
        }

        return null;
    }

    private Boolean ListContainsValue(List<String> listValues, String searchValue) {
        if (listValues == null || listValues.isEmpty() || String.isBlank(searchValue)) {
            return false;
        }

        for (String listValue :listValues) {
            if (searchValue.containsIgnoreCase(listValue) || listValue.containsIgnoreCase(searchValue)) {
                return true;
            }
        }

        return false;
    }
}