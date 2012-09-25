FMQLFILT; Caregraf - FMQL Filter Handling ; May 31st, 2012
    ;;0.95;FMQLQP;;May 31st, 2012
 
 
;
; FMQL Filters
; 
; Parse and Apply filters
; 
; FMQL Query Processor (c) Caregraf 2010-2012 AGPL
;
; TBD:
; - remove support for &: only &&
; - force all values to be quoted. Must be 'X' or "X"
; - remove need for \& inside quotes
;

;
; Simple & sequence of X op Y and bound(X)
;
; Note: must be careful with MUMPS evaluation precedence - lot's of brackets
;
FLTTOM(FLINF,FLT,IENA) 
    Q:$D(FLINF("BAD")) "0"
    N FLTP,FLTN,TST S FLTN=1,TST="("
    ; V0.9: temp support for & while allowing missing quotes
    S FLT=$$ESCAND(FLT) ; Escape & inside
    ; V0.9. Moving to &&. Still support & for now
    N AND S AND=$S(FLT["&&":"&&","1":"&") 
    F  S FLTP=$P(FLT,AND,FLTN) Q:FLTP=""  D
    . I FLTN>1 S TST=TST_"&"
    . S FLTN=FLTN+1
    . I $F(FLTP,"bound") S TST=TST_$$FLTBOUNDTOM(.FLINF,FLTP,$G(IENA))
    . E  S TST=TST_$$FLTLRTOM(.FLINF,$TR(FLTP,$C(0),"&"),$G(IENA))
    Q TST_")"
    
;
; Create executable expression from FMQL "X OP Y" filter element
;
; Arguments:
; - FLINF
; - FLT: the LVAL OP RVALUE filter
; - IENA mandatory for CNodes
; 
FLTLRTOM(FLINF,FLT,IENA) 
    N OP S OP=$S($F(FLT,"="):"=",$F(FLT,"["):"[",$F(FLT,">"):">",$F(FLT,"<"):"<",1:"")
    Q:OP="" "0"
    N FIELD S FIELD=$P(FLT,OP)
    N FDINF D BLDFDINF^FMQLUTIL(.FLINF,FIELD,.FDINF)
    Q:$D(FDINF("BAD")) "0"
    ; Take val from single quoted values after op
    N VAL S VAL=$P(FLT,OP,2)
    ; TBD: move to mandatory use of ' for values. Opt now for v0.8 compat
    ; Not using pattern. Allow quotes inside. Just catch at end.
    N QUOTE S QUOTE=$S($E(VAL,1)="'":"'",1:"""")
    ; Allow no quotes or ' or "
    I $E(VAL,1)=QUOTE,$E(VAL,$L(VAL))=QUOTE S VAL=$E(VAL,2,$L(VAL)-1)
    I OP=">",VAL'=+VAL S OP="]"  ; use order operator for non numeric > thans. TBD: what of <?
    Q:FDINF("TYPE")=8 "0"  ; not supporting VPTR yet
    S:FDINF("TYPE")=7 VAL=$P(VAL,"-",2)  ; remove file type if pointer
    S:FDINF("TYPE")=1 VAL=$$MAKEFMDATE^FMQLUTIL(VAL)  ; from FMQL to FM date
    N FAR S FARNA=$S($D(FLINF("GL")):$NA(FLINF("ARRAY")),"1":$NA(IENA))
    S LVAL="$P($G(@"_FARNA_"@(IEN,"""_FDINF("LOCSUB")_""")),""^"","_FDINF("LOCPOS")_")"
    ; One special case: remove file type if pointer. Second is date. Will do later.
    N MFLT S MFLT="("_LVAL_OP_""""_VAL_""")"
    Q MFLT
 
;
; Key aspect of FileMan (and sparcely populated stores in general). Optional
; fields don't get default values. One key filter is to check if a field has
; a value - is a field 'bound'?
;
FLTBOUNDTOM(FLINF,FLT,IENA) 
    N FIELD S FIELD=$P($P(FLT,"(",2),")")
    N FDINF D BLDFDINF^FMQLUTIL(.FLINF,FIELD,.FDINF)
    Q:$D(FDINF("BAD")) "0"
    N NOT S NOT=$S($F(FLT,"!"):"",1:"'")
    N FAR S FARNA=$S($D(FLINF("GL")):$NA(FLINF("ARRAY")),"1":$NA(IENA))
    ; CNode special. Bound if entries: ^LR(4,"CH",0)="^63.04D^38^38"
    ; Assume if 0 filled in then there are.
    Q:FDINF("TYPE")=9 "($G(@"_FARNA_"@(IEN,"""_FDINF("LOCSUB")_""",0))"_NOT_"="""")"
    Q "($P($G(@"_FARNA_"@(IEN,"""_FDINF("LOCSUB")_""")),""^"","_FDINF("LOCPOS")_")"_NOT_"="""")"
    
;
; Return Index of the filter. Without this, every filtered query becomes 
; a linear search.
;
; Note: Globals only
;
FLTIDX(FLINF,FLT,IDXA,IDXSTART) 
    Q:'$D(FLINF("GL"))
    N FLTP,FLTN
    S FLTN=1,IDXA="",IDXSTART=""
    S FLT=$$ESCAND(FLT) ; Escape inner &
    F  S FLTP=$P(FLT,"&",FLTN) Q:((FLTP="")!(IDXA'=""))  D
    . S FLTN=FLTN+1
    . Q:$F(FLTP,"bound")
    . N OP S OP=$S($F(FLT,"="):"=",$F(FLT,">"):">",1:"") Q:OP=""  ; =, > are only options
    . N FIELD S FIELD=$P(FLTP,OP)
    . N FDINF D BLDFDINF^FMQLUTIL(.FLINF,FIELD,.FDINF)
    . Q:$D(FDINF("BAD"))  ; Bad field
    . Q:FDINF("TYPE")=8  ; No "V" - not supporting variable pointers for now
    . N IDX S IDX=$$FIELDIDX^FMQLUTIL(FLINF("FILE"),FIELD)  ; TBD: move to meta
    . I IDX'="" D
    . . N ID S ID=$P(FLTP,OP,2)
    . . S ID=$TR(ID,$C(0),"&") ; put back escaped and
    . . N QUOTE S QUOTE=$S($E(ID,1)="'":"'",1:"""")
    . . ; Allow no quotes or ' or ". V0.9, force one or the other
    . . I $E(ID,1)=QUOTE,$E(ID,$L(ID))=QUOTE S ID=$E(ID,2,$L(ID)-1)
    . . Q:ID=""  ; Go to next possibility
    . . ; S:FDINF("TYPE")=4 ID=$E(ID,1,30) ; Indexes only count the first 30
    . . S:FDINF("TYPE")=7 ID=$P(ID,"-",2) ; FileMan internal form
    . . Q:ID=""  ; Not valid pointer. Catch of IDXA will be invalid
    . . S:FDINF("TYPE")=1 ID=$$MAKEFMDATE^FMQLUTIL(ID)  ; Internal date
    . . Q:ID=""  ; Not valid date
    . . I OP="=" S IDXA=FLINF("GL")_""""_IDX_""","""_ID_""")" Q
    . . ; Must be > as only option left 
    . . ; Special: float form that holds hhmmss gives problem as
    . . ; MUMPS $O won't navigate floats ie/ get the next in order.
    . . ; Must reduce date to base and get the extact next in order
    . . ; from that.
    . . S IDXA=FLINF("GL")_""""_IDX_""")"
    . . S IDXSTART=$S(FDINF("TYPE")=1:$O(@IDXA@($E(ID,1,7))),1:ID)
    Q

; Manual Index finding - TBD: move to own file ala manual SSAM
MFLTIDX(FLINF,FLT,IDXA,IDXSTART)
    I FLINF("FILE")="100" D OFLTIDX(.FLINF,FLT,.IDXA,.IDXSTART) Q
    D RXFLTIDX(.FLINF,FLT,.IDXA,.IDXSTART)
    Q
 
OFLTIDX(FLINF,FLT,IDXA,IDXSTART) 
    N FLTP,FLTN
    Q:FLINF("FILE")'="100"
    S IDXA="",IDXSTART=""
    S FLTN=1,IDXA=""
    F  S FLTP=$P(FLT,"&",FLTN) Q:FLTP=""  D
    . S FLTN=FLTN+1
    . I $F(FLTP,"bound") Q
    . N OP S OP=$S($F(FLT,"="):"=",1:"")
    . Q:OP=""
    . Q:OP'="="
    . N FIELD S FIELD=$P(FLTP,OP)
    . Q:FIELD'=".02"
    . I $P($P(FLTP,"=",2),"-")'="2" Q ; must be patient filter for now
    . N PID S PID=$P($P(FLTP,"=",2),"-",2) 
    . S IDXA="^OR(100,""AR"","""_PID_";DPT("")" Q
    Q

RXFLTIDX(FLINF,FLT,IDXA,IDXSTART)
    ; Rely on ^PS(55,PID,"P","A"... hidden RX index in the "P" multiple of 55
    N FLTP,FLTN
    Q:FLINF("FILE")'="52"
    S IDXA="",IDXSTART=""
    S FLTN=1,IDXA=""
    F  S FLTP=$P(FLT,"&",FLTN) Q:FLTP=""  D
    . S FLTN=FLTN+1
    . I $F(FLTP,"bound") Q
    . N OP S OP=$S($F(FLT,"="):"=",1:"")
    . Q:OP=""
    . Q:OP'="="
    . N FIELD S FIELD=$P(FLTP,OP)
    . Q:FIELD'="2"
    . I $P($P(FLTP,"=",2),"-")'="2" Q ; must be patient filter
    . N PID S PID=$P($P(FLTP,"=",2),"-",2)
    . S IDXA="^PS(55,"_PID_",""P"",""A"")" Q
    Q
 
; Escape & as long as support quoteless assertions
; FLT piece with form X\&Y
; Crude: some combinations (leading, trailing, multiple) won't work
; Will go when escaping becomes unnecessary in V1.0
ESCAND(FLTE) 
    N FLT S FLT=""
    N BIT,I S I=1
    F  S BIT=$P(FLTE,"\&",I) Q:BIT=""  D
    . S:I>1 FLT=FLT_$C(0)
    . S I=I+1
    . S FLT=FLT_BIT
    S:FLT="" FLT=EFLT
    Q FLT
