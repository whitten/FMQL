FMQLSCH ;CG/CD - Caregraf - FMQL Schema Query Processor; 04/04/2014  11:30
 ;;1.1;FMQLQP;;Apr 4th, 2014
 ;
 ; FMQL Schema Query Processor
 ;
 ; Companion of FMQLDATA - this resolves schema queries. Like its
 ; peer, it uses FLINF utilities rather than reading FM's raw dicts.
 ;
 ; FMQL Query Processor (c) Caregraf 2010-2014 AGPL
 ;
ALLTYPES(REPLY,FMQLPARAMS) ;
 N FILE,FILELABEL,TOPONLY,POPONLY,TCNT,CNT
 S TOPONLY=0,POPONLY=0,TCNT=0,CNT=0
 I $D(FMQLPARAMS("TOPONLY")),FMQLPARAMS("TOPONLY")="1" S TOPONLY=1
 I $D(FMQLPARAMS("POPONLY")),FMQLPARAMS("POPONLY")="1" S POPONLY=1
 D REPLYSTART^FMQLJSON(REPLY)
 S FILE=.109 ; allow .11 on but no .001 -> .1
 D LISTSTART^FMQLJSON(REPLY,"results")
 F  S FILE=$O(^DD(FILE)) Q:FILE'=+FILE  D
 . N FLINF D BLDFLINF^FMQLUTIL(FILE,.FLINF)  ; Important: initialize FLINF here
 . ; Will include WP which has ^DD entry but is not a file for FMQL
 . Q:$D(FLINF("BAD"))
 . S CNT=CNT+1
 . S:'$D(FLINF("PARENT")) TCNT=TCNT+1
 . I TOPONLY=1,$D(FLINF("PARENT")) Q
 . I POPONLY=1,'$D(FLINF("FMSIZE")) Q
 . D DICTSTART^FMQLJSON(REPLY)
 . D DASSERT^FMQLJSON(REPLY,"number",FILE)
 . D DASSERT^FMQLJSON(REPLY,"name",FLINF("LABEL"))
 . D:$D(FLINF("PARENT")) DASSERT^FMQLJSON(REPLY,"parent",FLINF("PARENT"))
 . D:$D(FLINF("GL")) DASSERT^FMQLJSON(REPLY,"global",FLINF("GL"))
 . D:$D(FLINF("FMSIZE")) DASSERT^FMQLJSON(REPLY,"count",FLINF("FMSIZE"))
 . D DICTEND^FMQLJSON(REPLY)
 . Q
 D LISTEND^FMQLJSON(REPLY)
 D DASSERT^FMQLJSON(REPLY,"allCount",CNT)
 D DASSERT^FMQLJSON(REPLY,"topCount",TCNT)
 ; Temporary (will move to a DESCRIBE SYSTEM. SITE and SITE LABEL
 D:$D(^DD("SITE")) DASSERT^FMQLJSON(REPLY,"siteLabel",^DD("SITE"))
 D:$D(^DD("SITE",1)) DASSERT^FMQLJSON(REPLY,"siteId",^DD("SITE",1))
 D DICTSTART^FMQLJSON(REPLY,"fmql")
 D DASSERT^FMQLJSON(REPLY,"OP","SELECT TYPES")
 I TOPONLY=1 D DASSERT^FMQLJSON(REPLY,"TOPONLY","true")
 I POPONLY=1 D DASSERT^FMQLJSON(REPLY,"POPONLY","true")
 D DICTEND^FMQLJSON(REPLY)
 D REPLYEND^FMQLJSON(REPLY)
 Q
 ;
BADTYPES(REPLY,FMQLPARAMS) ;
 N FILE,CNT,HASBFLD,FIELD
 S CNT=0
 D REPLYSTART^FMQLJSON(REPLY)
 S FILE=.109 ; allow .11 on but no .001 -> .1
 D LISTSTART^FMQLJSON(REPLY,"results")
 F  S FILE=$O(^DD(FILE)) Q:FILE'=+FILE  D
 . N FLINF D BLDFLINF^FMQLUTIL(FILE,.FLINF)  ; Important: initialize FLINF here
 . ; WP is a file (has DD entry) but not considered a file for FMQL
 . I $D(FLINF("BAD")),FLINF("BAD")="WP FILE" Q
 . I $D(FLINF("BAD")) D
 . . S CNT=CNT+1
 . . D DICTSTART^FMQLJSON(REPLY)
 . . D DASSERT^FMQLJSON(REPLY,"number",FILE)
 . . D DASSERT^FMQLJSON(REPLY,"corruption",FLINF("BAD"))
 . . D DICTEND^FMQLJSON(REPLY)
 . E  D
 . . S HASBFLD=0,FIELD=0 F  S FIELD=$O(^DD(FILE,FIELD)) Q:FIELD'=+FIELD  D
 . . . N FDINF D BLDFDINF^FMQLUTIL(.FLINF,FIELD,.FDINF)
 . . . I '$D(FDINF("BAD")) Q
 . . . I HASBFLD=0 D
 . . . . S HASBFLD=1
 . . . . S CNT=CNT+1
 . . . . D DICTSTART^FMQLJSON(REPLY)
 . . . . D DASSERT^FMQLJSON(REPLY,"number",FILE)
 . . . . D LISTSTART^FMQLJSON(REPLY,"badfields")
 . . . D DICTSTART^FMQLJSON(REPLY)
 . . . D DASSERT^FMQLJSON(REPLY,"number",FDINF("FIELD"))
 . . . D DASSERT^FMQLJSON(REPLY,"corruption",FDINF("BAD"))
 . . . D DICTEND^FMQLJSON(REPLY)
 . . I HASBFLD=1 D
 . . . D LISTEND^FMQLJSON(REPLY)
 . . . D DICTEND^FMQLJSON(REPLY)
 D LISTEND^FMQLJSON(REPLY)
 D DASSERT^FMQLJSON(REPLY,"badCount",CNT)
 D DICTSTART^FMQLJSON(REPLY,"fmql")
 D DASSERT^FMQLJSON(REPLY,"OP","DESCRIBE BADTYPES")
 D DICTEND^FMQLJSON(REPLY)
 D REPLYEND^FMQLJSON(REPLY)
 Q
 ;
ALLREFERRERSTOTYPE(REPLY,FMQLPARAMS) ;
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
 . Q:'$D(^DD(RFILE,0,"NM"))  ; rfile must be named
 . D DICTSTART^FMQLJSON(REPLY)
 . S RFILELABEL=$O(^DD(RFILE,0,"NM",""))
 . D DASSERT^FMQLJSON(REPLY,"rfile",RFILE)
 . D DASSERT^FMQLJSON(REPLY,"rfileLabel",RFILELABEL)
 . D LISTSTART^FMQLJSON(REPLY,"rfields")
 . S RFIELD="" F  S RFIELD=$O(^DD(FILE,0,"PT",RFILE,RFIELD)) Q:RFIELD'=+RFIELD  D
 . . Q:'$D(^DD(RFILE,RFIELD,0))  ; Skip Corruption
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
 ;
ERRORREPLY(REPLY,MSG) ;
 D REPLYSTART^FMQLJSON(REPLY)
 D DASSERT^FMQLJSON(REPLY,"error",MSG)
 D REPLYEND^FMQLJSON(REPLY)
 Q
 ;
DESCRIBETYPE(REPLY,FMQLPARAMS) ;
 I '$D(FMQLPARAMS("TYPE")) D ERRORREPLY(REPLY,"No File Type") Q
 N FILE S FILE=$TR(FMQLPARAMS("TYPE"),"_",".")
 N FLINF D BLDFLINF^FMQLUTIL(FILE,.FLINF)
 ; Note: corrupt file leads to an error with src of corruption
 I $D(FLINF("BAD")) D ERRORREPLY(REPLY,"Corrupt or Invalid File Type: "_FLINF("BAD")) Q
 I $D(FLINF("PARENT")) D SUBFILEINFO(REPLY,.FLINF) Q
 D TOPFILEINFO(REPLY,.FLINF)
 Q
 ;
TOPFILEINFO(REPLY,FLINF) ;
 D REPLYSTART^FMQLJSON(REPLY)
 D DASSERT^FMQLJSON(REPLY,"name",FLINF("LABEL"))
 D DASSERT^FMQLJSON(REPLY,"number",FLINF("FILE"))
 D DASSERT^FMQLJSON(REPLY,"location",FLINF("GL"))
 D:$D(FLINF("FMSIZE")) DASSERT^FMQLJSON(REPLY,"count",FLINF("FMSIZE"))
 D:$D(FLINF("LSTIEN")) DASSERT^FMQLJSON(REPLY,"lastIEN",FLINF("LSTIEN"))
 ; Leaving Descr outside FLINF as used so often.
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
 D FIELDSINFO(.FLINF)
 D DICTSTART^FMQLJSON(REPLY,"fmql")
 D DASSERT^FMQLJSON(REPLY,"OP","DESCRIBE TYPE")
 D DASSERT^FMQLJSON(REPLY,"TYPE",FLINF("EFILE"))
 D DASSERT^FMQLJSON(REPLY,"TYPELABEL",FLINF("LABEL"))
 D DICTEND^FMQLJSON(REPLY)
 D REPLYEND^FMQLJSON(REPLY)
 Q
 ;
SUBFILEINFO(REPLY,FLINF) ;
 D REPLYSTART^FMQLJSON(REPLY)
 D DASSERT^FMQLJSON(REPLY,"name",FLINF("LABEL"))
 D DASSERT^FMQLJSON(REPLY,"number",FLINF("FILE"))
 D DASSERT^FMQLJSON(REPLY,"parent",FLINF("PARENT"))
 ; TBD: Search CFILE for field that contains this one. Get field and description.
 D FIELDSINFO(.FLINF)
 D DICTSTART^FMQLJSON(REPLY,"fmql")
 D DASSERT^FMQLJSON(REPLY,"OP","DESCRIBE TYPE")
 D DASSERT^FMQLJSON(REPLY,"TYPE",FLINF("EFILE"))
 D DASSERT^FMQLJSON(REPLY,"TYPELABEL",FLINF("LABEL"))
 D DICTEND^FMQLJSON(REPLY)
 D REPLYEND^FMQLJSON(REPLY)
 Q
 ;
FIELDSINFO(FLINF) ;
 N FILE S FILE=FLINF("FILE")
 D LISTSTART^FMQLJSON(REPLY,"fields")
 S FIELD=0 F  S FIELD=$O(^DD(FILE,FIELD)) Q:FIELD'=+FIELD  D
 . N FDINF D BLDFDINF^FMQLUTIL(.FLINF,FIELD,.FDINF)
 . Q:$D(FDINF("BAD"))
 . D DICTSTART^FMQLJSON(REPLY)
 . D DASSERT^FMQLJSON(REPLY,"number",FDINF("FIELD"))
 . ; Send over all the flags. May process more on client side
 . D DASSERT^FMQLJSON(REPLY,"flags",FDINF("FLAGS"))
 . D DASSERT^FMQLJSON(REPLY,"name",FDINF("LABEL"))
 . ; UNIQPRED currently too costly
 . D DASSERT^FMQLJSON(REPLY,"pred",$$FIELDTOPRED^FMQLUTIL(FDINF("LABEL")))
 . D:$D(FDINF("LOC")) DASSERT^FMQLJSON(REPLY,"location",FDINF("LOC"))
 . ; For now, only note simple indexes. Not all ^DD(FILE,"IX",FIELD) as MUMPS there too
 . D:$D(FDINF("IDX")) DASSERT^FMQLJSON(REPLY,"index",FDINF("IDX"))
 . D:$D(FDINF("TRIGS")) DASSERT^FMQLJSON(REPLY,"triggers",FDINF("TRIGS"))
 . D:$D(FDINF("CREFNO")) DASSERT^FMQLJSON(REPLY,"crossReferenceCount",FDINF("CREFNO"))
 . D DASSERT^FMQLJSON(REPLY,"type",FDINF("TYPE"))
 . ; Extra details not in FDINF (yet)
 . N FLDDETAILS
 . I FDINF("TYPE")=9 S FLDDETAILS=+FDINF("FLAGS") ; Multiple
 . ; For now, pass back original values even for boolean
 . I ((FDINF("TYPE")=3)!(FDINF("TYPE")=12)) S FLDDETAILS=$P(^DD(FILE,FIELD,0),"^",3) ; Set
 . I FDINF("TYPE")=7 S FLDDETAILS=+$P(FDINF("FLAGS"),"P",2) ; Pointer
 . I FDINF("TYPE")=11,FDINF("FLAGS")["P" S FLDDETAILS=+$P(FDINF("FLAGS"),"P",2) ; Pointer (IEN pts)
 . ; TBD: Final FMQL won't distinguish vptr from ptr. MUMPS-side thing.
 . I FDINF("TYPE")=8 D
 . . S FLDDETAILS=$$VARPOINTERRANGE(FILE,FIELD) ; V Pointer
 . D:$D(FLDDETAILS) DASSERT^FMQLJSON(REPLY,"details",FLDDETAILS)
 . ; TODO: move into FDINF as useful for filters
 . I $L($P(^DD(FILE,FIELD,0),"^",5)) D
 . . ; TODO: calculate better - using length to get over all internal ^
 . . N CALC S CALC=$P(^DD(FILE,FIELD,0),"^",5,$L(^DD(FILE,FIELD,0)))
 . . Q:CALC="Q"
 . . N CALCTYPE S CALCTYPE=$S(FDINF("TYPE")=6:"computation",1:"inputTransform")
 . . D DASSERT^FMQLJSON(REPLY,CALCTYPE,CALC)
 . D:$D(FDINF("HIDE")) DASSERT^FMQLJSON(REPLY,"hidden","true")
 . ; Keeping WP here. Not useful for checking and could be big.
 . I $D(^DD(FILE,FIELD,21,1))  D
 . . D WPASTART^FMQLJSON(REPLY,"DESCRIPTION","-1")
 . . N I S I=0 F  S I=$O(^DD(FILE,FIELD,21,I)) Q:I'=+I  D
 . . . D WPALINE^FMQLJSON(REPLY,^DD(FILE,FIELD,21,I,0))
 . . . Q
 . . D WPAEND^FMQLJSON(REPLY)
 . D DICTEND^FMQLJSON(REPLY)
 D LISTEND^FMQLJSON(REPLY)
 Q
 ;
 ; TODO: use FMQLUTIL's instead (PLOC reassemble)
VARPOINTERRANGE(FILE,FIELD) ;
 N X,VPS,VP
 S VPS=""
 I '$D(^DD(FILE,FIELD,"V")) Q VPS ; TBD ERROR
 S X=0 F  S X=$O(^DD(FILE,FIELD,"V",X)) Q:X'=+X  D
 . S VP=$P(^DD(FILE,FIELD,"V",X,0),"^",1)
 . I VPS'="" S VPS=VPS_";"
 . S VPS=VPS_VP
 Q VPS
 ;

