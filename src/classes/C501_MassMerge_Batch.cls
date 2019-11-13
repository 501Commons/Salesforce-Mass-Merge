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

public class C501_MassMerge_Batch implements Database.Batchable<sObject> { 

  public String query;
  public Boolean AccountMerge = false;
  public Boolean ContactMerge = false;

  public Database.querylocator start(Database.BatchableContext BC) {
    C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.DEBUG, '***** C501_MassMerge_Batch::start - query: ' + query);
    return Database.getQueryLocator(query);
  }

  public void execute(Database.BatchableContext info, List<sObject> scope) {
    
    if (AccountMerge) {
      List<Account> accountDuplicates = new List<Account>();
      Account masterAccount = null;
      for (sObject mergeObject :scope) {
        C501_Account_Merge__c mergeCandidate = (C501_Account_Merge__c)mergeObject;

        masterAccount = new Account( Id = mergeCandidate.Account__c );
        accountDuplicates.add(new Account( Id = mergeCandidate.Merge_Target_Account__c ));
      }

      C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.DEBUG, '***** C501_MassMerge_Batch::execute - masterAccount: ' + String.valueOf(masterAccount));
      C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.DEBUG, '***** C501_MassMerge_Batch::execute - accountDuplicates: ' + String.valueOf(accountDuplicates));
      C501_MassMerge_SharedCode_Account c501MassMerge_SharedCode_Account = new C501_MassMerge_SharedCode_Account();

      try
      {
        c501MassMerge_SharedCode_Account.ObjectsMerge(masterAccount, accountDuplicates);
      }
      catch (C501_MassMerge_SharedCode_Account.C501_MassMerge_SharedCode_AccountException customException)
      {
        C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.ERROR, '***** C501_MassMerge_Batch::execute - C501_MassMerge_SharedCode_AccountException: ' + String.valueOf(customException.getMessage()));
      }
      catch (System.DmlException dmlException) {
        C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.DEBUG, '***** C501_MassMerge_Batch::execute - System.DmlException: ' + String.valueOf(dmlException.getMessage()));
      }
      catch (Exception generalException) {
        C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.DEBUG, '***** C501_MassMerge_Batch::execute - Exception: ' + String.valueOf(generalException.getMessage()));
      }
    }
    else if (ContactMerge) {
      List<Contact> contactDuplicates = new List<Contact>();
      Contact masterContact = null;
      for (sObject mergeObject :scope) {
        C501_Contact_Merge__c mergeCandidate = (C501_Contact_Merge__c)mergeObject;

        masterContact = new Contact( Id = mergeCandidate.Contact__c );
        contactDuplicates.add(new Contact( Id = mergeCandidate.Merge_Target_Contact__c ));
      }

      C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.DEBUG, '***** C501_MassMerge_Batch::execute - masterContact: ' + String.valueOf(masterContact));
      C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.DEBUG, '***** C501_MassMerge_Batch::execute - contactDuplicates: ' + String.valueOf(contactDuplicates));
      C501_MassMerge_SharedCode_Contact c501MassMerge_SharedCode_Contact = new C501_MassMerge_SharedCode_Contact();
      c501MassMerge_SharedCode_Contact.ObjectsMerge(masterContact, contactDuplicates);
    }
  }

  public void finish(Database.BatchableContext info) {
  }
}