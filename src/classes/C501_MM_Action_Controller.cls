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

public with sharing class C501_MM_Action_Controller {

    public Id Id { get; set; }
    public C501_Merge_Metric__c mergeMetric { get; set; }

    public C501_MM_Action_Controller(ApexPages.StandardController controller) {

        mergeMetric =  (C501_Merge_Metric__c)controller.getRecord();
        Id = mergeMetric.Id;
    }

    //Method that can is called from the Visual Force page action attribute
    public PageReference discoverTrigger() {

        System.debug(LoggingLevel.DEBUG, '*****C501_MM_Action_Controller::discoverTrigger Id: ' + Id);
        C501_MM_Rule_Discover_Accounts.executeMetricFuture(Id);
        C501_MM_Rule_Discover_ContactsAccount.executeMetricFuture(Id);
        C501_MM_Rule_Discover_ContactsAccounts.executeMetricFuture(Id);

        System.debug(LoggingLevel.DEBUG, '*****C501_MM_Action_Controller::discoverTrigger After executeMetric');

        PageReference pageRef = new PageReference('/' + Id);
        pageRef.setRedirect(true);

        System.debug(LoggingLevel.DEBUG, '*****C501_MM_Action_Controller::discoverTrigger After PageReference');

        // Returns to the original page
        return pageRef;
    }

    //Method that can is called from the Visual Force page action attribute
    public PageReference mergeTrigger() {

        System.debug(LoggingLevel.DEBUG, '*****C501_MM_Action_Controller::mergeTrigger Id: ' + Id);
        C501_MM_Rule_Merge.get().executeMetric(Id);

        PageReference pageRef = new PageReference('/' + Id);
        pageRef.setRedirect(true);

        // Returns to the original page
        return pageRef;
    }
}