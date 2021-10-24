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

public without sharing class C501_MassMerge_Fx_Base {
    public C501_MassMerge_Fx_Base() {

    }

    // Save date each time called and if not called in 24 hours then turn off schedule
    // Update a custom object list; Id, IdType, FieldName, Partial (8).
    //    Example: For Contact with FirstName equals Partial Match and FirstName is David. 
    //    C501_Contact_Match, C501_Account_Match, C501_Merge_Match (list of merge matches - if not updated in 24 hours then remove)
    //    Master/Detail so if Contact delete the partials are deleted
    //      00O2g000000JJEq, Partial Match, Contact, FirstName, d, 20.0
    //      00O2g000000JJEq, Partial Match, Contact, FirstName, da, 15.0
    //      00O2g000000JJEq, Partial Match, Contact, FirstName, dav, 10.0
    //      00O2g000000JJEq, Partial Match, Contact, FirstName, davi, 5.0
    //      00O2g000000JJEq, Partial Match, Contact, FirstName, david, 0.0

    // Need a field on the contact and on the account - C501_MassMerge_LastChecked__c - this is what you use to get a DESC list and that a limit of to process on each schedule run
    // during schedule run process each Fx - for instance if Partial Match and Fuzzy Match both enabled for FirstName then loop through both Fx classes per contact for firstname.
    // have a Fx base collection that you loop through.
}