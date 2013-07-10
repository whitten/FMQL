FMQLUTIL;Caregraf - FMQL Utilities ; Jul, 2013
 ;;1.0;FMQLQP;;Jul 12th, 2013
 ;
 ; FMQL Utilities
 ; 
 ; Schema and data array walkers and checkers
 ; 
 ; FMQL Query Processor (c) Caregraf 2010-2013 AGPL
 ;
 ;
 ;
 ; eXecute a routine, TOX (TO eXecute) on members of a file. Flexible - TOX
 ; can do anything from a straightforward exposure of file contents to 
 ; counting to aggregations of various kinds.
 ; - for both globals and cnodes
 ;   - IENA="" if GL - ie/ only set for CNODE
 ; - Controls position in file with LIMIT, OFFSET, AFTERIEN
 ; - PARAMS allows extra values to be passed to TOX
 ; - NOIDXMX = Maximum size of file to filter where no index exists. -1 means no max. This
 ;   matters for filters. You don't want to make a linear search of a huge file looking 
 ;   for a couple of entries. Note: not being set in MUMPS - upper bound set in Apache
 ; - Special case: ORDERBY - for now only on .01 if the B Index
 ;
XONFL(FLINF,FILTER,IENA,LIMIT,OFFSET,AFTERIEN,ORDERBY,NOIDXMX,TOX,PARAMS) 
 N PLC,MFLT
 I AFTERIEN'="" S OFFSET=0  ; Ensure OFFSET off if AFTERIEN
 S ^TMP("FMQLE","AFTERIEN")=AFTERIEN
 S PLC("LIMIT")=LIMIT,PLC("OFFLFT")=OFFSET,PLC("AFTERIEN")=AFTERIEN,PLC("CNT")=0
 ; Build filter expression
 S MFLT=$S(FILTER'="":"S MFTEST="_$$FLTTOM^FMQLFILT(.FLINF,FILTER,IENA),1:"")
 ; CNodes walk IENA.
 I '$D(FLINF("GL")) D XIENA(.FLINF,IENA,MFLT,.PLC,TOX,.PARAMS) Q PLC("CNT")
 ; Special case: ORDER BY .01 and BIDX supported
 I ORDERBY=".01",$D(FLINF("BIDX")) D XIDXA(.FLINF,FLINF("BIDX"),"",MFLT,.PLC,TOX,.PARAMS) Q PLC("CNT")
 ; Global but no filter - walk IENA
 I MFLT="" D XIENA(.FLINF,"","",.PLC,TOX,.PARAMS) Q PLC("CNT")
 ; See if filter yields an IDXA(V)
 D FLTIDX^FMQLFILT(.FLINF,FILTER,.IDXA,.IDXSTART)
 ; 5 Cases:
 ; - a. non > filter gives IDXAV (IDXSTART="")
 I $G(IDXA)'="",$G(IDXSTART)="" D XIENA(.FLINF,IDXA,MFLT,.PLC,TOX,.PARAMS) Q PLC("CNT")
 ; - b. > filter gives IDXA (IDXSTART'="")
 I $G(IDXA)'="" D XIDXA(.FLINF,IDXA,IDXSTART,MFLT,.PLC,TOX,.PARAMS) Q PLC("CNT")
 ; - c. manual IDXA (100,52). Though = filter, still use XIDXA. No MFLT
 D MFLTIDX^FMQLFILT(.FLINF,FILTER,.IDXA,.IDXSTART)
 I $G(IDXA)'="" D XIDXA(.FLINF,IDXA,"","",.PLC,TOX,.PARAMS) Q PLC("CNT")
 ; - d. No IDXA(V) but filter. See if file too big to filter
 I NOIDXMX'=-1,($S($D(FLINF("FMSIZE")):FLINF("FMSIZE")>NOIDXMX,1:1)) Q -1
 ; - e. file not to big to filter, row by row
 D XIENA(.FLINF,"",MFLT,.PLC,TOX,.PARAMS)
 Q PLC("CNT")
 ;
 ;
 ; Apply TOX on entries in a simple IEN Array, a file in IEN order
 ; 
 ; Used for plain walks of files in IEN order, for contained node walks and 
 ; for non-indexed filtering of smaller files.
 ;
XIENA(FLINF,IENA,MFLT,PLC,TOX,PARAMS) 
 N FAR,AIEN,IEN,MFTEST
 S FAR=$S($D(FLINF("GL")):FLINF("ARRAY"),1:IENA)  ; Global or CNode
 ; Assumption: OFFLFT=0 if AFTERIEN as it takes precedence
 S AIEN=$S($D(PLC("AFTERIEN")):PLC("AFTERIEN"),1:0)
 S IEN=AIEN F  S IEN=$O(@FAR@(IEN)) Q:IEN'=+IEN!(PLC("CNT")=PLC("LIMIT"))  D
 . Q:($P($G(@FAR@(IEN,0)),"^")="")  ; All must have .01 value
 . I MFLT'="" X MFLT Q:'MFTEST  ; Quit if filter fails
 . I PLC("OFFLFT")>0 S PLC("OFFLFT")=PLC("OFFLFT")-1 Q  ; Quit if not at offset
 . S PLC("CNT")=PLC("CNT")+1
 . X TOX  ; Takes .FLINF, IEN, FAR (for CNodes), PARAMS (extras)
 Q
 ;
 ;
 ; An IDX Value Array (IDXAV) is more involved than a simple IEN array
 ; - IDX's can embed alias' ex/ ^DPT("B",NAME,IEN,"X")=1
 ; - IENs may not be in the leaf/last position
 ; 
 ; Used for equality filters where the predicate asserted is indexed. Key 
 ; for efficiently traversing the graph arrangements (Vital points to Patient)
 ; 
XIDXAV(FLINF,IDXAV,MFLT,PLC,TOX,PARAMS) 
 N FAR,AIEN,IEN,MFTEST
 I '$D(FLINF("GL")) Q -1  ; globals only, CNodes walked in XIENA
 S FAR=FLINF("ARRAY")  ; ^DPT() not ^DPT(
 ; Assumption: OFFLFT=0 if AFTERIEN as it takes precedence
 S AIEN=$S($D(PLC("AFTERIEN")):PLC("AFTERIEN"),1:0)
 I '$D(PLC("LIEN")) S PLC("LIEN")=AIEN
 S IEN=AIEN F  S IEN=$O(@IDXAV@(IEN)) Q:IEN'=+IEN!(PLC("CNT")=PLC("LIMIT"))  D
 . Q:IEN=PLC("LIEN")  ; Traverse above leaves means same IEN > once in order
 . S PLC("LIEN")=IEN  ; Need to track across walks if 2 step IDXA
 . Q:$G(@IDXAV@(IEN))'=""  ; Skip all aliases. Aliases appear out of order
 . Q:($P($G(@FAR@(IEN,0)),"^")="")  ; All must have .01 value
 . I MFLT'="" X MFLT Q:'MFTEST  ; Quit if filter fails
 . I PLC("OFFLFT")>0 S PLC("OFFLFT")=PLC("OFFLFT")-1 Q  ; Quit if not at offset
 . S PLC("CNT")=PLC("CNT")+1
 . X TOX  ; Takes .FLINF, IEN, FAR (for CNodes), PARAMS (extras)
 Q
 ;
 ;
 ; IDX Array traversal is a two stepper: walk the array in value order and
 ; step down into the value arrays.
 ;
 ; Used for > filters and ORDERBY (which is equivalent to > "") 
 ;
XIDXA(FLINF,IDXA,IDXSTART,MFLT,PLC,TOX,PARAMS)
 N IDXV,IDXVA
 S IDXV=IDXSTART F  S IDXV=$O(@IDXA@(IDXV)) Q:IDXV=""  D
 . S IDXVA=$NA(@IDXA@(IDXV))
 . D XIDXAV(.FLINF,IDXVA,MFLT,.PLC,TOX,.PARAMS)
 Q
 ;
 ;
 ; File's are globals (T files) or subfiles (S files)
 ;
BLDFLINF(FILE,FLINF) ;
 S FILE=$TR(FILE,"_",".")
 S FLINF("FILE")=FILE
 S FLINF("EFILE")=$TR(FILE,".","_")
 I '$D(^DD(FILE)) S FLINF("BAD")="No such file" Q
 I '$D(^DD(FILE,.01,0)) S FLINF("BAD")=".01 corrupt" Q
 I $D(^DIC(FILE,0,"GL")) D BLDTFINF(FILE,.FLINF) Q
 I $G(^DD(FILE,0,"UP"))'="" D BLDSFINF(FILE,.FLINF) Q
 S FLINF("BAD")="No global or multiple definition"
 Q
 ;
 ;
 ; Top File Info
 ; Fields: ARRAY, BIDX, FILE, FLAGS, FMSIZE, GL, LABEL
 ;
BLDTFINF(FILE,FLINF) ;
 I $G(^DIC(FILE,0))="" S FLINF("BAD")="^DIC 0 Has No Data" Q
 S FLINF("GL")=^DIC(FILE,0,"GL")
 ; Handle ^DPT( and ^GMR(120.5,
 S FLINF("ARRAY")=$E(FLINF("GL"),1,$L(FLINF("GL"))-1)
 I FLINF("ARRAY")["(" S FLINF("ARRAY")=FLINF("ARRAY")_")"
 ; S FLINF("ARRAY")=$TR(FLINF("GL"),",",")")
 I '$D(@FLINF("ARRAY")@(0)) S FLINF("BAD")="No 0 Entry for Array" Q
 S FLHDR=@FLINF("ARRAY")@(0)
 I $P(FLHDR,"^")="" S FLINF("BAD")="No Name" Q
 ; TODO: have just LABEL. Do non native changes above this.
 ; S FLINF("LABEL")=$TR($P(FLHDR,"^"),"/","_")  ; alt is ^DD(FILE,0,"NM")
 S FLINF("LABEL")=$P(FLHDR,"^")
 S FLINF("FLAGS")=$P(FLHDR,"^",2)
 ; don't always have size
 I $P(FLHDR,"^",4) S FLINF("FMSIZE")=+$P(FLHDR,"^",4)
 I $P(FLHDR,"^",3) S FLINF("LSTIEN")=$P(FLHDR,"^",3)
 ; Version information
 S:$D(^DD(FILE,0,"VR")) FLINF("VERSION")=^DD(FILE,0,"VR")
 S:$D(^DD(FILE,0,"VRPK")) FLINF("VPACKAGE")=^DD(FILE,0,"VRPK")
 ; Not sending VRRV as formats vary - ex/ 80 vs 798.1
 I $D(^DIC(FILE,"%",1))  D  ; APP GROUPS
 . S APGSVAL=""
 . S I=0 F  S I=$O(^DIC(FILE,"%",I)) Q:I'=+I  D 
 . . I APGSVAL'="" S APGSVAL=APGSVAL_";"
 . . S APGSVAL=APGSVAL_^DIC(FILE,"%",I,0)
 . . Q
 . S FLINF("APPGRPS")=APGSVAL
 ; I $D(@FLINF("ARRAY")@("B")) S FLINF("BIDX")=FLINF("GL")_"""B"")"
 ; S:$P($G(@FLINF("ARRAY")@(.01,1,1,0)),"^",2)="B" FLINF("BIDX")=FLINF("GL")_"""B"")"
 S:$$FIELDIDX(FILE,".01")="B" FLINF("BIDX")=FLINF("GL")_"""B"")"
 ; # of fields is: $P(^DD(FILE,0),"^",0)
 Q
 ;
 ;
 ; Sub or contained file Info
 ; Most (all?) sub file's named in files ie. 120_81 inside 120_8 but can't 
 ; key off that.
 ; FIELDS: FILE, LABEL, PARENT, PFIELD, PLOCSUBS
 ;
BLDSFINF(FILE,FLINF) ;
 ; Ex/ ^DD(8925.02,.01,0)="REPORT TEXT^W^^0;1^Q"
 ; Used '$$VFILE^DILFD(FILE) elsewhere to same effect
 ; WP is a file (has DD entry) but not considered a file for FMQL
 I $P($G(^DD(FILE,.01,0)),"^",2)["W" S FLINF("BAD")="WP FILE" Q
 I '$D(^DD(FILE,0,"NM")) S FLINF("BAD")="No Name" Q
 S FLINF("LABEL")=$O(^DD(FILE,0,"NM",""))
 S FLINF("PARENT")=^DD(FILE,0,"UP")
 ; TODO: check slow down but needed to get at Subfile Array anyhow
 N PFLINF D BLDFLINF(FLINF("PARENT"),.PFLINF)
 I $D(PFLINF("BAD")) S FLINF("BAD")="Corrupt Parent: "_PFLINF("BAD") Q
 I '$D(^DD(FLINF("PARENT"),"SB",FILE)) S FLINF("BAD")="Parent doesn't know this multiple" Q
 ; Get Field by Sub File id and not by sub file label in "B"
 S FLINF("PFIELD")=$O(^DD(FLINF("PARENT"),"SB",FILE,""))  ; SubFile location in parent
 I '$D(^DD(FLINF("PARENT"),FLINF("PFIELD"),0)) S FLINF("BAD")="Multiple doesn't know parent's field for it" Q
 S PLOCPOS=$P(^DD(FLINF("PARENT"),FLINF("PFIELD"),0),"^",4)
 I PLOCPOS="" S FLINF("BAD")="No location information" Q
 I $P(PLOCPOS,";",2)'="0" S FLINF("BAD")="Multiple not in position 0" Q
 S FLINF("PLOCSUB")=$P(PLOCPOS,";")
 Q
 ;
 ;
 ; Field Info
 ; Fields: FIELD, FLAGS, LABEL, LOCPOS, LOCSUB, TYPE
 ; Specials fields: CODES (for type 3)
 ; 
 ; TODO: 
 ; - Careful: gfs_frm.htm not definite. Ex/ "S" in flags if multiple with only
 ; one field, a set of codes (ex/ 120.506S for ^DD(120.5,4,0)
 ; - Computed (C) is DC,BC,C,Cm,Cmp. Must distinguish actual type. Correlate with no location
 ; - move inputTransform in here from Sch serializer. Want for filter processing
 ; - move IDX in here from Sch serializer: want for filters
 ; - Add ^DD(FILE,FIELD,1,1,...)
 ;
BLDFDINF(FLINF,FIELD,FDINF) ;
 N FILE S FILE=FLINF("FILE")
 S FIELD=$TR(FIELD,"_",".")
 S FDINF("FIELD")=FIELD
 I '$D(^DD(FILE,FIELD,0)) S FDINF("BAD")="No 0 Definition: "_FILE_"/"_FIELD Q
 N FLAGS S FLAGS=$P(^DD(FILE,FIELD,0),"^",2)
 S FDINF("FLAGS")=FLAGS
 S FDINF("LABEL")=$P(^DD(FILE,FIELD,0),"^")
 ; Pred: use in XML fields/RDF and JSON. TODO: account for name reuse
 S FDINF("PRED")=$$FIELDTOPRED(FDINF("LABEL"))
 ; Date/Number/Codes/String/WP String/Pointer/V Pointer/MULT/MUMPS
 ; TBD: Computed - B, m, D
 I +FLAGS D  ; WP and MULT both refer to subfile in flags.
 . I $P($G(^DD(+FLAGS,.01,0)),"^",2)["W" S FDINF("TYPE")=5
 . E  S FDINF("TYPE")=9 S FDINF("SUBFILE")=+FLAGS  ; TBD: validate ["M ?
 ; TBD: Default String even if no "F". Should log.
 E  D
 . ; Even though .001 has a type, it is also "computed" so treat that way
 . S FDINF("TYPE")=$S(FIELD=.001:6,FLAGS["D":1,FLAGS["N":2,FLAGS["S":3,FLAGS["F":4,FLAGS["C":6,FLAGS["P":7,FLAGS["V":8,FLAGS["K":10,1:"4") ; Default to String
 . N IDX S IDX=$$FIELDIDX^FMQLUTIL(FILE,FIELD)
 . S:IDX'="" FDINF("IDX")=IDX
 ; TODO: this BAD is never reached as type defaults to String
 I FDINF("TYPE")="" S FDINF("BAD")="No type set: "_FILE_"/"_FIELD Q
 ; Access, Verify in file 200 are sensitive. FM should support this formally and encrypt them
 I FILE=200,((FIELD=2)!(FIELD=11)) S FDINF("HIDE")="SENSITIVE"
 I FDINF("TYPE")'=6 D
 . S FDLOC=$P(^DD(FILE,FIELD,0),"^",4) 
 . S FDINF("LOC")=FDLOC
 . S FDINF("LOCSUB")=$P(FDLOC,";") 
 . ; Check for " ; "? ie. spaces even though field not given as computed
 . I $TR(FDINF("LOCSUB")," ")="" S FDINF("BAD")="Corrupt location: "_FILE_"/"_FIELD Q
 . ; Position of 9 is 1 but that's meaningless. Leave out position.
 . I FDINF("TYPE")'=9 D
 . . N LOCWHERE S LOCWHERE=$P(FDLOC,";",2)
 . . I LOCWHERE="" S FDINF("BAD")="No location position: "_FILE_"/"_FIELD Q
 . . ; Extract form for 63/.1 (E1,19) or 68/.1;E1,220 (limit for screenman?)
 . . I LOCWHERE?1"E"1.N1","1.N S FDINF("LOCE")=$E(LOCWHERE,1,$L(LOCWHERE)) Q
 . . I LOCWHERE=+LOCWHERE S FDINF("LOCPOS")=LOCWHERE Q
 . . ; TBD: is there another position type? Return an error until I support it.
 . . S FDINF("BAD")="Unsupported location position: "_FILE_"/"_FIELD_":"_LOCWHERE Q
 I FDINF("TYPE")=3 D 
 . N CODES S CODES=$P(^DD(FILE,FIELD,0),"^",3)
 . I CODES="" S FDINF("BAD")="No codes specified: "_FILE_"/"_FIELD Q
 . N C F C=1:1 Q:$P(CODES,";",C)=""  S FDINF("CODES",$P($P(CODES,";",C),":"))=$P($P(CODES,";",C),":",2)
 I FDINF("TYPE")=7 S FDINF("PFILE")=+$P(FLAGS,"P",2) Q
 I FDINF("TYPE")=9 S FDINF("BFILE")=+FLAGS Q
 I FDINF("TYPE")=8 D
 . I '$D(^DD(FILE,FIELD,"V")) S FDINF("BAD")="No VPTR Definition: "_FILE_"/"_FIELD Q
 . S X=0 F  S X=$O(^DD(FILE,FIELD,"V",X)) Q:X'=+X  D
 . . N PFILE S PFILE=$P(^DD(FILE,FIELD,"V",X,0),"^",1)
 . . I '$D(^DIC(PFILE,0,"GL")) S FDINF("BAD")="No Global for VPTR target: "_PFILE Q
 . . N PLOC S PLOC=^DIC(PFILE,0,"GL")
 . . S FDINF("PLOC",PLOC)=PFILE
 . . S FDINF("PFILE",PFILE)=PLOC
 Q
 ;
 ;
 ; Get first non-mumps index for a field.
 ; TBD: check ^DD(FILE,"IX",FIELD) - compare to walk below
 ; TBD: support .11 ie. Walk its ^DD("IX","B",FILE#) or ? ie. XREFs defined outside file. Equivalent of DESRIBE _11 FILTER(.01=[FILE]&&.2=R) and look at fields. Will need to distinguish .11 INDEX from Simple like B. Use array.
 ; TBD: move ORDER special in here (it is a MUMPS old ref). 
 ; TBD: future - meta format to define select MUMPS indexes for use here.
 ;
FIELDIDX(FILE,FIELD) ;
 N IDXID,IDXINF,IDX
 I FILE=8927.1,FIELD=.01 Q "B" ; Missing from TIU TEMPLATE FIELD ^DD
 S IDX=""
 ; From '$D(^DD(FILE,FIELD,1))
 I '$D(^DD(FILE,FIELD,1,1)) Q ""  ; indexes number 1 up
 S IDXID=0 F  S IDXID=$O(^DD(FILE,FIELD,1,IDXID)) Q:((IDXID'=+IDXID)!(IDX'=""))  D  ; > 0 has indexes
 . Q:'$D(^DD(FILE,FIELD,1,IDXID,0))  ; TBD Corruption note
 . S IDXINF=^DD(FILE,FIELD,1,IDXID,0)
 . I $P(IDXINF,"^",3)'="MUMPS" S IDX=$P(^DD(FILE,FIELD,1,IDXID,0),"^",2) Q
 Q IDX
 ;
 ;
 ; Get External Value
 ; TBD: get vptr
 ; TBD: GETS maps some .01's (50_605 to field 1 etc.) Is this in meta or ?
 ; Another ex is 120_8 Allergy Type is a 4 but treated like a CODE. Ext form
 ; comes from ^DD(120.8,3.1,2.1)="S Y=$$OUTTYPE^GMRAUTL(Y)" [this doesn't work
 ; for lab's name map.]
 ; TBD: catch the invalid - CODES beyond range, bad ptrs, dates etc.
 ;
GETEVAL(FDINF,IVAL) ;
 Q:$D(FDINF("HIDE")) "**HIDDEN**"
 Q:FDINF("TYPE")=1 $$MAKEXMLDATE^FMQLUTIL(IVAL)
 I FDINF("TYPE")=3,$D(FDINF("CODES",IVAL)) Q FDINF("CODES",IVAL)
 N EVAL S EVAL=IVAL ; Fallback to internal value
 I FDINF("TYPE")=7 D
 . I IVAL="0" Q ; TBD Common NULL value that doesn't resolve
 . N PFLINF D BLDFLINF(FDINF("PFILE"),.PFLINF)
 . Q:$D(PFLINF("BAD"))
 . N PFDINF D BLDFDINF(.PFLINF,.01,.PFDINF)
 . Q:$D(PFDINF("BAD"))
 . Q:$G(@PFLINF("ARRAY")@(IVAL,0))="" ; Invalid Pointer
 . S IVAL=$P(@PFLINF("ARRAY")@(IVAL,0),"^")
 . Q:IVAL=""
 . S EVAL=$$GETEVAL(.PFDINF,IVAL)
 ; VPTR very like PTR - once PFILE is know.
 I FDINF("TYPE")=8 D
 . N PLOC S PLOC="^"_$P(IVAL,";",2)
 . Q:'$D(FDINF("PLOC",PLOC)) ; TBD: catch the buggy ptr instead
 . N PFILE S PFILE=FDINF("PLOC",PLOC)
 . N PFLINF D BLDFLINF(PFILE,.PFLINF)
 . Q:$D(PFLINF("BAD"))
 . N PID S PID=$P(IVAL,";")
 . Q:$G(@PFLINF("ARRAY")@(PID,0))="" ; Invalid Pointer
 . N PFDINF D BLDFDINF(.PFLINF,.01,.PFDINF)
 . Q:$D(PFDINF("BAD"))
 . N PIVAL S PIVAL=$P(@PFLINF("ARRAY")@(PID,0),"^")
 . Q:PIVAL=""
 . S EVAL=$$GETEVAL(.PFDINF,PIVAL)
 Q EVAL
 ;
 ;
 ; Parse URL
 ; TBD: CNode - S PFILE=$G(^DD(FILE,0,"UP")) Q:'PFILE . Without this
 ; can't do recursive INs or DESCRIBE ONE of CNode.
 ; 
PARSEURL(URL,FLINF,ID) ;
 N FILE S FILE=$P(URL,"-")
 D BLDFLINF(FILE,.FLINF)
 I $D(FLINF("BAD")) Q
 S ID=$P(URL,"-",2) ; TBD: support CNode Identification
 Q
 ;
 ;
 ; Form: YYMMDD.HHMMSS but no trailing 0's ie 3 not 03 for hour if no minutes
 ;
MAKEXMLDATE(FMDATE) ;
 N XMLDATE
 S XMLDATE=(+$E(FMDATE,1,3)+1700)
 Q:$L(FMDATE)<4 XMLDATE_"-00-00T00:00:00Z"
 S XMLDATE=XMLDATE_"-"_$S($E(FMDATE,5)="":"0"_$E(FMDATE,4),1:$E(FMDATE,4,5))
 Q:$L(FMDATE)<6 XMLDATE_"-00T00:00:00Z"
 S XMLDATE=XMLDATE_"-"_$S($E(FMDATE,7)="":"0"_$E(FMDATE,6),1:$E(FMDATE,6,7))_"T"
 Q:$L(FMDATE)<9 XMLDATE_"00:00:00Z"
 S XMLDATE=XMLDATE_$S($E(FMDATE,10)="":"0"_$E(FMDATE,9),1:$E(FMDATE,9,10))_":"
 Q:$L(FMDATE)<11 XMLDATE_"00:00Z"
 S XMLDATE=XMLDATE_$S($E(FMDATE,12)="":"0"_$E(FMDATE,11),1:$E(FMDATE,11,12))_":"
 Q:$L(FMDATE)<13 XMLDATE_"00Z"
 S XMLDATE=XMLDATE_$S($E(FMDATE,14)="":"0"_$E(FMDATE,13),1:$E(FMDATE,13,14))_"Z"
 Q XMLDATE
 ;
 ;
 ; XMLDATE: YYYY-MM-DDTHH:MM:SSZ
 ; Note: over current RPC, will be YYYY-MM-DDTHH-MM-SSZ but
 ; behaves the same as not validating separators below.
 ; TBD: validate date contents correct (see X ^DD(DD) etc)
 ; Possible Issue: 2010-02 -> ...2, not ...02 in FileMan. 
 ; There may be month-only dates in FileMan and in this case,
 ; 3102 ie/ first month of 2010 will be less (numerically)
 ; than 310100. But month-wise it is more. In effect, month
 ; only or year only dates need per file example handling.
 ;
MAKEFMDATE(XMLDATE) ;
 N FMDATE
 ; If missing month or day, then pad with 0s
 S FMDATE=(+$E(XMLDATE,1,4)-1700)
 Q:$L(XMLDATE)<7 FMDATE_"0000"
 S FMDATE=FMDATE_$E(XMLDATE,6,7)
 Q:$L(XMLDATE)<10 FMDATE_"00"
 S FMDATE=FMDATE_$E(XMLDATE,9,10)
 Q:$E(XMLDATE,11)'="T" FMDATE
 S FMDATE=FMDATE_"."
 ; If trailing info missing, ok as $E returns "". 
 ; Does mean will accept 3 for 03 which isn't correct XML
 S FMDATE=FMDATE_$E(XMLDATE,12,13)
 S FMDATE=FMDATE_$E(XMLDATE,15,16)
 S FMDATE=FMDATE_$E(XMLDATE,18,19)
 Q FMDATE
 ;
 ;
 ; Predicate is lower alphanum and _
 ;
FIELDTOPRED(FIELD) ;
 SET ALW="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz_ /"
 SET UPC="ABCDEFGHIJKLMNOPQRSTUVWXYZ /"
 SET LOC="abcdefghijklmnopqrstuvwxyz__"
 S PRED=$TR($TR(FIELD,$TR(FIELD,ALW)),UPC,LOC)
 Q PRED
 ;
