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

@isTest
public class C501_MM_ListView_Controller_TEST {
    
    //==================== TEST METHOD(s) ======================================

    @isTest(SeeAllData='true')
    public static void DiscoverTests() {

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        List<C501_Merge_Metric__c> mergeMetrics = Database.Query(strSoql);

        Test.StartTest();

        ApexPages.StandardSetController standardController = new ApexPages.StandardSetController(mergeMetrics);
        C501_MM_ListView_Controller controller = new C501_MM_ListView_Controller(standardController);

        controller.discoverTrigger();   

        Test.StopTest();
    }

    @isTest(SeeAllData='true')
    public static void MergeTests() {

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        List<C501_Merge_Metric__c> mergeMetrics = Database.Query(strSoql);

        Test.StartTest();

        ApexPages.StandardSetController standardController = new ApexPages.StandardSetController(mergeMetrics);
        C501_MM_ListView_Controller controller = new C501_MM_ListView_Controller(standardController);

        controller.mergeTrigger();   

        Test.StopTest();
    }
}