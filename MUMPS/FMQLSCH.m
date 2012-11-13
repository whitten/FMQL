FMQLSCH; Caregraf - FMQL Schema Query Processor ; May 31st, 2012
    ;;0.96;FMQLQP;;Nov 12, 2012
 
; FMQL Schema Query Processor
; 
; Companion of FMQLDATA - this resolves schema queries. Like its
; peer, it uses FLINF utilities rather than reading FM's raw dicts.
; 
; FMQL Query Processor (c) Caregraf 2010-2012 AGPL
 
ALLTYPES(REPLY,FMQLPARAMS)  
    N FILE,FILELABEL
    N TOPONLY S TOPONLY=0
    I $D(FMQLPARAMS("TOPONLY")),FMQLPARAMS("TOPONLY")="1" S TOPONLY=1
    D REPLYSTART^FMQLJSON(REPLY)
    S FILE=0 ; allow .11 etc ie/ all the system setup stuff.
    D LISTSTART^FMQLJSON(REPLY,"results")
    F  S FILE=$O(^DD(FILE)) Q:FILE'=+FILE  D
    . N FLINF D BLDFLINF^FMQLUTIL(FILE,.FLINF)
    . Q:$D(FLINF("BAD"))
    . I TOPONLY,$D(FLINF("PARENT")) Q
    . D DICTSTART^FMQLJSON(REPLY)
    . D DASSERT^FMQLJSON(REPLY,"name",FLINF("LABEL"))
    . D DASSERT^FMQLJSON(REPLY,"number",FLINF("FILE"))
    . D:$D(FLINF("PARENT")) DASSERT^FMQLJSON(REPLY,"parent",FLINF("PARENT"))
    . D:$D(FLINF("GL")) DASSERT^FMQLJSON(REPLY,"global",FLINF("GL"))
    . D:$D(FLINF("FMSIZE")) DASSERT^FMQLJSON(REPLY,"count",FLINF("FMSIZE"))
    . D DICTEND^FMQLJSON(REPLY)
    . Q
    D LISTEND^FMQLJSON(REPLY)
    D DICTSTART^FMQLJSON(REPLY,"fmql")
    D DASSERT^FMQLJSON(REPLY,"OP","SELECT TYPES")
    I TOPONLY=1 D DASSERT^FMQLJSON(REPLY,"TOPONLY","true")
    D DICTEND^FMQLJSON(REPLY)
    D REPLYEND^FMQLJSON(REPLY)
    Q
 
ALLREFERRERSTOTYPE(REPLY,FMQLPARAMS) 
    N FILE,FILELABEL,RFILE,RFILELABEL,RFIELD,RFIELDLABEL
    I '$D(FMQLPARAMS("TYPE")) D ERRORREPLY(REPLY,"No File Type") Q
    S FILE=$TR(FMQLPARAMS("TYPE"),"_",".")
    I '$D(^DIC(FILE,0,"GL")) D ERRORREPLY(REPLY,"Invalid Global File Type: "_FILE) Q  ; top level only
    D REPLYSTART^FMQLJSON(REPLY)
    S FILELABEL=$O(^DD(FILE,0,"NM",""))
    D DASSERT^FMQLJSON(REPLY,"fmqlFileName",FILELABEL) ; always file name back
    D LISTSTART^FMQLJSON(REPLY,"results")
    S RFILE="" F  S RFILE=$O(^DD(FILE,0,"PT",RFILE)) Q:RFILE'=+RFILE  D  ; Order ala IEN Order
    . Q:'$D(^DIC(RFILE,0,"GL"))  ; TBD: only do globals for now
    . Q:'$D(^DD(RFILE,0,"NM")) ; rfile must be named
    . D DICTSTART^FMQLJSON(REPLY)
    . S RFILELABEL=$O(^DD(RFILE,0,"NM",""))
    . D DASSERT^FMQLJSON(REPLY,"rfile",RFILE)
    . D DASSERT^FMQLJSON(REPLY,"rfileLabel",RFILELABEL)
    . D LISTSTART^FMQLJSON(REPLY,"rfields")
    . S RFIELD="" F  S RFIELD=$O(^DD(FILE,0,"PT",RFILE,RFIELD)) Q:RFIELD'=+RFIELD  D
    . . Q:'$D(^DD(RFILE,RFIELD,0)) ; Skip Corruption
    . . D DICTSTART^FMQLJSON(REPLY)
    . . S RFIELDLABEL=$P(^DD(RFILE,RFIELD,0),"^") ; TBD quit if no label
    . . D DASSERT^FMQLJSON(REPLY,"rfield",RFIELD)
    . . D DASSERT^FMQLJSON(REPLY,"rfieldLabel",RFIELDLABEL)
    . . D DICTEND^FMQLJSON(REPLY)
    . . Q
    . D LISTEND^FMQLJSON(REPLY)
    . D DICTEND^FMQLJSON(REPLY)
    . Q
    D LISTEND^FMQLJSON(REPLY)
    D DICTSTART^FMQLJSON(REPLY,"fmql")
    D DASSERT^FMQLJSON(REPLY,"OP","SELECT TYPE REFS")
    D DASSERT^FMQLJSON(REPLY,"TYPE",$TR(FILE,".","_"))
    D DASSERT^FMQLJSON(REPLY,"TYPELABEL",FILELABEL)
    D DICTEND^FMQLJSON(REPLY)
    D REPLYEND^FMQLJSON(REPLY)
    Q
 
ERRORREPLY(REPLY,MSG) 
    D REPLYSTART^FMQLJSON(REPLY) 
    D DASSERT^FMQLJSON(REPLY,"error",MSG) 
    D REPLYEND^FMQLJSON(REPLY)
    Q
 
DESCRIBETYPE(REPLY,FMQLPARAMS)
    I '$D(FMQLPARAMS("TYPE")) D ERRORREPLY(REPLY,"No File Type") Q
    N FILE S FILE=$TR(FMQLPARAMS("TYPE"),"_",".")
    N FLINF D BLDFLINF^FMQLUTIL(FILE,.FLINF)
    I $D(FLINF("BAD")) D ERRORREPLY(REPLY,"Invalid File Type: "_FMQLPARAMS("TYPE")) Q
    I $D(FLINF("PARENT")) D SUBFILEINFO(REPLY,.FLINF) Q
    D TOPFILEINFO(REPLY,.FLINF)
    Q
 
TOPFILEINFO(REPLY,FLINF) 
    D REPLYSTART^FMQLJSON(REPLY)
    D DASSERT^FMQLJSON(REPLY,"name",FLINF("LABEL"))
    D DASSERT^FMQLJSON(REPLY,"number",FLINF("FILE"))
    D DASSERT^FMQLJSON(REPLY,"location",FLINF("GL"))
    D:$D(FLINF("FMSIZE")) DASSERT^FMQLJSON(REPLY,"count",FLINF("FMSIZE"))
    D:$D(FLINF("LSTIEN")) DASSERT^FMQLJSON(REPLY,"lastIEN",FLINF("LSTIEN"))
    ; Leaving Descr outside FLINF for now. Must get dirty.
    I $D(^DIC(FILE,"%D",0))  D  ; DESCRIPTION
    . D WPASTART^FMQLJSON(REPLY,"DESCRIPTION","-1")
    . S I=0 F  S I=$O(^DIC(FILE,"%D",I)) Q:I'=+I  D
    . . D WPALINE^FMQLJSON(REPLY,^DIC(FILE,"%D",I,0))
    . . Q
    . D WPAEND^FMQLJSON(REPLY)
    . Q
    D:$D(FLINF("APPGRPS")) DASSERT^FMQLJSON(REPLY,"applicationGroups",FLINF("APPGRPS"))
    D:$D(FLINF("VERSION")) DASSERT^FMQLJSON(REPLY,"version",FLINF("VERSION"))
    D:$D(FLINF("VPACKAGE")) DASSERT^FMQLJSON(REPLY,"vpackage",FLINF("VPACKAGE"))
    D FIELDSINFO(FLINF("FILE"))
    D DICTSTART^FMQLJSON(REPLY,"fmql")
    D DASSERT^FMQLJSON(REPLY,"OP","DESCRIBE TYPE")
    D DASSERT^FMQLJSON(REPLY,"TYPE",FLINF("EFILE"))
    D DASSERT^FMQLJSON(REPLY,"TYPELABEL",FLINF("LABEL"))
    D DICTEND^FMQLJSON(REPLY)
    D REPLYEND^FMQLJSON(REPLY)
    Q

SUBFILEINFO(REPLY,FLINF)
    D REPLYSTART^FMQLJSON(REPLY)
    D DASSERT^FMQLJSON(REPLY,"name",FLINF("LABEL"))
    D DASSERT^FMQLJSON(REPLY,"number",FLINF("FILE"))
    D DASSERT^FMQLJSON(REPLY,"parent",FLINF("PARENT"))
    ; TBD: Search CFILE for field that contains this one. Get field and description.
    D FIELDSINFO(FLINF("FILE"))
    D DICTSTART^FMQLJSON(REPLY,"fmql")
    D DASSERT^FMQLJSON(REPLY,"OP","DESCRIBE TYPE")
    D DASSERT^FMQLJSON(REPLY,"TYPE",FLINF("EFILE"))
    D DASSERT^FMQLJSON(REPLY,"TYPELABEL",FLINF("LABEL"))
    D DICTEND^FMQLJSON(REPLY)
    D REPLYEND^FMQLJSON(REPLY)
    Q
 
; TODO: move all to FLDINFO
FIELDSINFO(FILE) 
    N I
    D LISTSTART^FMQLJSON(REPLY,"fields")
    S FIELD=0 F  S FIELD=$O(^DD(FILE,FIELD)) Q:FIELD'=+FIELD  D 
    . Q:'$D(^DD(FILE,FIELD,0))
    . D DICTSTART^FMQLJSON(REPLY)
    . D DASSERT^FMQLJSON(REPLY,"number",FIELD)
    . S FLDFLAGS=$P(^DD(FILE,FIELD,0),"^",2) ; includes type - V,P etc.
    . D DASSERT^FMQLJSON(REPLY,"flags",FLDFLAGS)
    . ; Add ^DD(FILE,FIELD,1,1,...)
    . ; For now, only note simple indexes. Not all ^DD(FILE,"IX",FIELD) as MUMPS there too
    . S IDX=$$FIELDIDX^FMQLUTIL(FILE,FIELD)
    . D:IDX'="" DASSERT^FMQLJSON(REPLY,"index",IDX)
    . S FLDLABEL=$P(^DD(FILE,FIELD,0),"^")
    . ; TODO: remove name == predicate once worked through. Make a client thing
    . D DASSERT^FMQLJSON(REPLY,"name",$$FIELDTOPRED^FMQLUTIL(FLDLABEL))
    . ; TODO: rename this straight filename label -> name to match file
    . D DASSERT^FMQLJSON(REPLY,"label",FLDLABEL)
    . S FLDLOC=$P(^DD(FILE,FIELD,0),"^",4)
    . D:FLDLOC'=" ; " DASSERT^FMQLJSON(REPLY,"location",FLDLOC) ; Computed has "no location"
    . ; Careful: gfs_frm.htm not definite. Ex/ "S" in flags if multiple with only
    . ; one field, a set of codes (ex/ 120.506S for ^DD(120.5,4,0)
    . K FLDTYPE,FLDDETAILS
    . I +FLDFLAGS D  ; MULTIPLE or WORD PROCESSING
    . . ; To go direct - if "W" in flags of $P(FLDFLAGS,"P"). See function.
    . . I '$$VFILE^DILFD(+FLDFLAGS) S FLDTYPE=5 ; don't care about WP's "multiple"
    . . E  S FLDTYPE=9 S FLDDETAILS=+FLDFLAGS ; Multiple
    . E  D
    . . I FLDFLAGS["D" S FLDTYPE=1 ; Date 
    . . I FLDFLAGS["N" S FLDTYPE=2 ; Numeric
    . . I FLDFLAGS["S" S FLDTYPE=3 S FLDDETAILS=$P(^DD(FILE,FIELD,0),"^",3) ; Set
    . . I FLDFLAGS["F" S FLDTYPE=4 ; Free Text
    . . ; TBD: Final FMQL must isolate mumps properly.
    . . I FLDFLAGS["K" S FLDTYPE=10 ; MUMPS
    . . I FLDFLAGS["P" S FLDTYPE=7 S FLDDETAILS=+$P(FLDFLAGS,"P",2) ; Pointer
    . . ; TBD: Final FMQL won't distinguish vptr from ptr. MUMPS-side thing.
    . . I FLDFLAGS["V" S FLDTYPE=8 S FLDDETAILS=$$VARPOINTERRANGE(FILE,FIELD) ; V Pointer
    . . ; TBD: Computed (C) is DC,BC,C,Cm,Cmp. Must distinguish actual type. Correlate with no location.
    . . I '$D(FLDTYPE) S FLDTYPE=6 ; Computed: TBD: Break to BC, Cm, DC, C ie. qualifier
    . . I $L($P(^DD(FILE,FIELD,0),"^",5)) D
    . . . ; TODO: calculate better - using length to get over all internal ^
    . . . N CALC S CALC=$P(^DD(FILE,FIELD,0),"^",5,$L(^DD(FILE,FIELD,0)))
    . . . Q:CALC="Q"
    . . . N CALCTYPE S CALCTYPE=$S(FLDTYPE=6:"computation",1:"inputTransform")
    . . . D DASSERT^FMQLJSON(REPLY,CALCTYPE,CALC)
    . . Q
    . D DASSERT^FMQLJSON(REPLY,"type",FLDTYPE)
    . D:$D(FLDDETAILS) DASSERT^FMQLJSON(REPLY,"details",FLDDETAILS)
    . I $D(^DD(FILE,FIELD,21,1))  D
    . . D WPASTART^FMQLJSON(REPLY,"DESCRIPTION","-1")
    . . S I=0 F  S I=$O(^DD(FILE,FIELD,21,I)) Q:I'=+I  D 
    . . . D WPALINE^FMQLJSON(REPLY,^DD(FILE,FIELD,21,I,0))
    . . . Q
    . . D WPAEND^FMQLJSON(REPLY)
    . . Q
    . D DICTEND^FMQLJSON(REPLY)
    D LISTEND^FMQLJSON(REPLY)
    Q
 
VARPOINTERRANGE(FILE,FIELD) 
    N X,VPS,VP
    S VPS=""
    I '$D(^DD(FILE,FIELD,"V")) Q VPS ; TBD ERROR
    S X=0 F  S X=$O(^DD(FILE,FIELD,"V",X)) Q:X'=+X  D
    . S VP=$P(^DD(FILE,FIELD,"V",X,0),U,1)
    . I VPS'="" S VPS=VPS_";"
    . S VPS=VPS_VP
    Q VPS
