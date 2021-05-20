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
// GENERIC CLASS - modify the C501_MM_ListView_[Object]_Controller.cls classes then copy to the all the others
//
//      1) Make changes to the class
//      2) Copy/Paste entire class to all other C501_MassMerge_SharedCode_[Object].cls classes
//      3) Search and replace all [Object] references with [Target Object]
//
//  This is a workaround for Generic classes since <T> - Templating not supported in Apex
//

@isTest
public class C501_MM_ListView_AccountM_CTRL_TEST {
    
    //==================== TEST METHOD(s) ======================================

    @isTest(SeeAllData='true')
    public static void MergeTests() {

      C501_MM_ListView_AccountM_CTRL_TEST.runTest('Master');
    }

    @isTest(SeeAllData='true')
    public static void IgnoreTests() {

      C501_MM_ListView_AccountM_CTRL_TEST.runTest('Ignore');
    }    

    private static void runTest(String mergeAction) {

      List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Account Test');
      List<Account> listAccounts = [SELECT Id FROM Account WHERE Name LIKE '%TESTDATA%'];

        C501_Account_Merge__c objectMerge = new C501_Account_Merge__c(
                Unique_Id__c = 'TESTDATA Name ' + listAccounts[0].Id,
                Merge_Metric__c = mergeMetrics[0].Id,
                Account__c = listAccounts[0].Id,
                Merge_Target_Account__c = listAccounts[1].Id,
                Merge_Confidence__c = 100.0);

        insert objectMerge;

        Test.StartTest();

        ApexPages.StandardSetController standardController = new ApexPages.StandardSetController(new List<C501_Account_Merge__c>());
        C501_MM_ListView_AccountM_CTRL controller = new C501_MM_ListView_AccountM_CTRL(standardController);

        String strSoql = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
        if (String.isBlank(strSoql)) {
          return;
        }

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MM_ListView_AccountM_CTRL_TEST::MergeTests strSoql: ' + strSoql);
        List<C501_Account_Merge__c> recordMerges = Database.query(strSoql);

        System.assert(recordMerges.size() > 0);
  
        controller.recordId = recordMerges[0].Account__c;

        if (mergeAction.equalsIgnoreCase('Master')) {
          controller.mergeTrigger();
        }
        else {
          controller.ignoreTrigger();
        }

        Test.StopTest();

        List<C501_Account_Merge__c> recordMergesAfter = Database.query(strSoql);

        System.assert(recordMerges.size() <> recordMergesAfter.size(), 'Merges Before: ' + recordMerges.size() + ' Merges After: ' + recordMergesAfter.size());
    }
}