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

public without sharing class C501_MM_ListView_Controller {

    public C501_MM_ListView_Controller(ApexPages.StandardSetController controller) {
    }

    public PageReference discoverTrigger() {

        // User initiated call by clicking the Discover button so clear out potential contact merges to do a fresh start
        //  Need to delete contacts merges here before calling the 2 seperate contact futures, account merges are cleared out in the account future call since there is only 1
        //
        List<C501_Contact_Merge__c> deleteContactMerges = [SELECT Id from C501_Contact_Merge__c WHERE IsDeleted = false];
        if (!deleteContactMerges.isEmpty()) {
          Database.DeleteResult[] deleteResults = Database.delete(deleteContactMerges, false);
          for (Database.DeleteResult deleteResult :deleteResults) {
              if (!deleteResult.isSuccess()) {
                  for (Database.Error resultError : deleteResult.getErrors()) {
                      C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::discoverTrigger');
                  }
              }
          }
        }

        C501_MM_Rule_Discover_Accounts.executeFuture();
        C501_MM_Rule_Discover_ContactsAccount.executeFuture();
        C501_MM_Rule_Discover_ContactsAccounts.executeFuture();

        return new ApexPages.Action('{!List}').invoke();
    }

    public PageReference mergeTrigger() {

        C501_MM_Rule_Merge.get().execute(null);

        return new ApexPages.Action('{!List}').invoke();
    }
}