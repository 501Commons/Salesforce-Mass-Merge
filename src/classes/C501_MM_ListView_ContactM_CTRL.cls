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

public without sharing class C501_MM_ListView_ContactM_CTRL {

    @testVisible private final List<C501_Contact_Merge__c> selectedMerges;
    @testVisible private String recordId;

    public boolean redirect { get; private set; }
    public ID idRedirect { get; private set; }

    public C501_MM_ListView_ContactM_CTRL(ApexPages.StandardSetController controller) {

      this.selectedMerges = (List<C501_Contact_Merge__c>)controller.getSelected();
      this.recordId = ApexPages.currentPage().getParameters().get('id');

      this.redirect = true;
      this.idRedirect = this.recordId;     
    }

    public PageReference mergeTrigger() {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::mergeTrigger ');
        this.processMerge('Master');

        PageReference pageRef = new PageReference('/' + this.idRedirect);
        pageRef.setRedirect(true);

        // Returns to the original page
        return pageRef;
    }

    public PageReference ignoreTrigger() {

      C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ignoreTrigger ');
      this.processMerge('Ignore');

      return new ApexPages.Action('{!List}').invoke();
  }

    private void processMerge(String mergeAction) {

      C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::processMerge - mergeAction: ' + mergeAction);

      String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
      if (String.isBlank(strSoql)) {
        return;
      }

      if (this.selectedMerges.size() > 0) {
        strSoql += ' AND Id = \'' + this.selectedMerges[0].Id + '\' LIMIT 1';
      }
      else {
        strSoql += ' AND (Contact__c = \'' + this.recordId + '\' OR Merge_Target_Contact__c = \'' + this.recordId + '\') LIMIT 1';
      }

      C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::mergeTrigger strSoql: ' + strSoql);
      List<C501_Contact_Merge__c> recordMerges = Database.query(strSoql);

      if (recordMerges.size() <= 0) {
        return;
      }

      recordMerges[0].Merge_Action__c = mergeAction;
      if (!String.valueOf(recordMerges[0].Contact__c).equalsIgnoreCase(this.recordId)){
        recordMerges[0].Merge_Target_Contact__c = recordMerges[0].Contact__c;
        recordMerges[0].Contact__c = this.recordId;
      }

      Database.SaveResult[] updateResults = Database.update(recordMerges, false);
      for (Database.SaveResult updateResult :updateResults) {
          if (!updateResult.isSuccess()) {
              for(Database.Error resultError : updateResult.getErrors()) {
                  C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::processMerge');
              }                        
          }
      }

      if (mergeAction.equalsIgnoreCase('Master')) {
        C501_MassMerge_SharedCode_Contact c501MassMerge_SharedCode_Contact = new C501_MassMerge_SharedCode_Contact();
        c501MassMerge_SharedCode_Contact.ProcessObjectMerges(new List<C501_Merge_Metric__c>(), recordMerges);
      }

      return;
    }
}