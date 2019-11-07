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

trigger C501_Merge_MetricTrigger on C501_Merge_Metric__c (after update) {

    // Merge Metric changed so delete all the merge candidates
    Set<Id> mergeMetricIds = new Set<Id>();
    for (C501_Merge_Metric__c mergeMetricId :trigger.new) {

        C501_Merge_Metric__c updatedRecord = Trigger.newMap.get(mergeMetricId.Id);
        C501_Merge_Metric__c previousRecord = Trigger.oldMap.get(mergeMetricId.Id);

        if (updatedRecord.Discover_AutoMerge_Objects_Only__c <> previousRecord.Discover_AutoMerge_Objects_Only__c ||
            updatedRecord.Enable_Account_Merge_Discovery__c <> previousRecord.Enable_Account_Merge_Discovery__c ||
            updatedRecord.Enable_Contact_Merge_Discovery__c <> previousRecord.Enable_Contact_Merge_Discovery__c ||
            updatedRecord.AutoMerge_Objects__c <> previousRecord.AutoMerge_Objects__c) {
            
            System.debug(LoggingLevel.DEBUG, 'C501_Merge_MetricTrigger change detected in a field related to merge candidates');
            mergeMetricIds.add(mergeMetricId.Id);
        }
    }
    
    if (!mergeMetricIds.isEmpty() && !Test.isRunningTest()) {
        C501_MassMerge_SharedCode.DeleteMergeMetricChildren(mergeMetricIds);
    }
}