/*
    Nonprofit Salesforce © 2022 by 501 Commons is licensed under CC BY 4.0
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

trigger C501_Merge_MetricTrigger on C501_Merge_Metric__c (before delete, after update) {

    if (trigger.isDelete) {
      for (C501_Merge_Metric__c mergeMetric :trigger.old) {

          List<C501_Contact_Merge__c> contactMerges = [SELECT Id FROM C501_Contact_Merge__c WHERE Merge_Action__c = 'Ignore' AND Merge_Metric__c = :mergeMetric.Id LIMIT 1];
          List<C501_Account_Merge__c> accountMerges = [SELECT Id FROM C501_Account_Merge__c WHERE Merge_Action__c = 'Ignore' AND Merge_Metric__c = :mergeMetric.Id LIMIT 1];
          if (contactMerges.size() > 0 || accountMerges.size() > 0) {
            mergeMetric.addError('Delete Error - 1 or more Ignore records exist for merge metric');
          }

          continue;
        }

      return;
    }

    // Merge Metric changed so delete all the merge candidates
    Set<Id> mergeMetricChangeIds = new Set<Id>();
    for (C501_Merge_Metric__c mergeMetric :trigger.new) {

        C501_Merge_Metric__c updatedRecord = Trigger.newMap.get(mergeMetric.Id);
        C501_Merge_Metric__c previousRecord = Trigger.oldMap.get(mergeMetric.Id);

        if (updatedRecord.Discovery_Objects__c <> previousRecord.Discovery_Objects__c ||
            updatedRecord.AutoMerge_Objects__c <> previousRecord.AutoMerge_Objects__c) {
            
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, 'C501_Merge_MetricTrigger change detected in a field related to merge candidates updatedRecord: ' + updatedRecord);
            if (updatedRecord.Discovery_Objects__c <> previousRecord.Discovery_Objects__c) {
              C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, 'C501_Merge_MetricTrigger - Discovery_Objects__c - current: ' + updatedRecord.Discovery_Objects__c + ' previous: ' + previousRecord.Discovery_Objects__c);
            }
            if (updatedRecord.AutoMerge_Objects__c <> previousRecord.AutoMerge_Objects__c) {
              C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, 'C501_Merge_MetricTrigger - AutoMerge_Objects__c - current: ' + updatedRecord.AutoMerge_Objects__c + ' previous: ' + previousRecord.AutoMerge_Objects__c);
            }
            mergeMetricChangeIds.add(mergeMetric.Id);
        }
    }
    
    if (!mergeMetricChangeIds.isEmpty() && !Test.isRunningTest()) {
        C501_MassMerge_SharedCode.DeleteMergeMetricChildren(mergeMetricChangeIds);
    }
}