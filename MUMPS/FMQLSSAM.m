FMQLSSAM ;CG/CD - Caregraf - FMQL Schema Enhancement for Terminologies; 07/12/2013  11:30
 ;;1.0;FMQLQP;;Jul 12th, 2013
 ;
 ;
 ; FMQL Schema Enhancement for Terminologies - sameas relationships
 ;
 ; FMQL Query Processor (c) Caregraf 2010-2013 AGPL
 ;
 ; Link (declare sameas) local vocabulary/resources to standard or national 
 ; equivalents. This is part of FMQL's FileMan schema enhancements and 
 ; all vocabs identified in the vocabulary graph should be processed here.
 ;
 ; Get SAMEAS of: LOCAL (no national map where there might be), LOCAL:XX-XX
 ; a local only map, VA:xxxx (usually VUIDs), NLT64 (for Lab only)
 ;
 ; Note: Like other enhancements, the logic could be migrated to FileMan's 
 ; schema. Computed pointers called "sameas" could be added to relevant files.
 ;
 ; TODO: 
 ; - VA: NDC (11 form)
 ; - DEFLABEL passed in for VUID. Lookup explicitly with better .01 routine
 ;
 ; IMPORTANT: pass in empty SAMEAS
 ;
RESOLVE(FILENUM,IEN,DEFLABEL,SAMEAS) ;
 Q:'$D(^DIC(FILENUM,0,"GL"))  ; catches CNode
 Q:IEN'=+IEN  ; catch non numeric IEN
 ; Could just check $D(^PSDFDB)
 I ^TMP($J,"NS")="VS" D RESVS(FILENUM,IEN,DEFLABEL,.SAMEAS) Q
 D RESC(FILENUM,IEN,DEFLABEL,.SAMEAS)  ; C*** Vocabs
 Q
 ;
 ;
 ; VS Resolution
 ;
RESVS(FILENUM,IEN,DEFLABEL,SAMEAS) ;
 I FILENUM="50.7" D RESVS50dot7(IEN,.SAMEAS) Q  ; PHARMACY ORDERABLE
 I FILENUM="50" D RESVS50(IEN,.SAMEAS) Q  ; DRUG
 I FILENUM="71" D RESVS71(IEN,.SAMEAS) Q  ; RAD/NUC PROCEDURE
 I FILENUM="790.2" D RESVS790dot2(IEN,.SAMEAS) Q  ; WV PROCEDURE
 I FILENUM="757" D RESVS757(IEN,.SAMEAS) Q  ; Major Concept
 I FILENUM="757.01" D RESVS757dot01(IEN,.SAMEAS) Q  ; EXP
 I FILENUM="9999999.27" D RESVS9999999dot27(IEN,.SAMEAS) Q  ; Prov Nav
 I FILENUM="60" D RESVS60(IEN,.SAMEAS) Q  ; Lab Local
 I FILENUM="64" D RESVS64(IEN,.SAMEAS) Q  ; Lab National WKLD
 I FILENUM="200" D RESVS200(IEN,.SAMEAS) Q  ; NPI for Providers
 D RESVSVAFIXED(FILENUM,IEN,DEFLABEL,.SAMEAS) Q:$D(SAMEAS("URI"))
 D RESVSSTANDARD(FILENUM,IEN,DEFLABEL,.SAMEAS) Q:$D(SAMEAS("URI"))
 D RESVSVUID(FILENUM,IEN,DEFLABEL,.SAMEAS) Q:$D(SAMEAS("URI"))
 Q
 ;
 ;
 ; If VUID-ed file entry has no VUID then will return "LOCAL" as sameAs value 
 ;
 ; TBD: VUIDs for many coded-value fields, all given in 8985_1. Can LU with NS
 ;      CR _11-537. Ex/ YES/NO fields etc. File#,Field#,IVALUE leads to VUID
 ;      Note: complication of 63_04. Could local enum labs get VUIDs too?
RESVSVUID(FILENUM,IEN,DEFLABEL,SAMEAS) ;
 N VUID,VUIDE
 I (($G(DEFLABEL)="")!($G(IEN)="")) Q  ; RESVSDRUG, maybe more need this
 Q:'$D(^DD(FILENUM,"B","VUID",99.99))
 S SAMEAS("URI")="LOCAL"
 S VUIDE=^DIC(FILENUM,0,"GL")_IEN_","_"""VUID"""_")"
 I DEFLABEL["/" S DEFLABEL=$TR(DEFLABEL,"/","-")  ; TMP: names with /. TBD fix.
 I $G(@VUIDE) S SAMEAS("URI")="VA:"_$P($G(@VUIDE),"^",1) S SAMEAS("LABEL")=DEFLABEL  Q
 Q
 ;
 ; Fixed files: 5,11,13 no VUID but VA standard
 ; Note: 10(.1/.2) not in here. CDC/HL7 coded.
 ; Unlike VUIDes will make SAMEAS take same value as IEN.
RESVSVAFIXED(FILENUM,IEN,DEFLABEL,SAMEAS) ;
 I FILENUM="5"!(FILENUM="11")!(FILENUM="13")  D
 . S SAMEAS("URI")="VA:"_$TR(FILENUM,".","_")_"-"_IEN
 . S SAMEAS("LABEL")=$P(DEFLABEL,"/",2)
 Q 
 ;
 ; Standard files: 80 (ICD), 81 (CPT), 8932.1 (Provider Codes)
 ; TBD: 95.3 (LOINC), SNOMED/RT 61 ...
 ; ISSUE: must intercept LOINC BEFORE try VUID sameas
 ; These should never be local - unless in error
RESVSSTANDARD(FILENUM,IEN,DEFLABEL,SAMEAS) ;
 ; No default local: should all have codes!
 I FILENUM="80" D
 . Q:'$D(@("^ICD9("_IEN_",0)"))  ; TBD: log invalid
 . S SAMEAS("URI")="ICD9CM:"_$P(DEFLABEL,"/",2)
 . S SAMEAS("LABEL")=$P(@("^ICD9("_IEN_",0)"),"^",3)
 I FILENUM="81" D
 . Q:'$D(@("^ICPT("_IEN_",0)"))
 . S SAMEAS("URI")="CPT:"_$P(DEFLABEL,"/",2)
 . S SAMEAS("LABEL")=$P(@("^ICPT("_IEN_",0)"),"^",2)
 I FILENUM="8932.1" D 
 . Q:'$D(^USC(8932.1,IEN,0))
 . S SAMEAS("URI")="LOCAL" ; inactives lack codes
 . N X12CODE S X12CODE=$P(^USC(8932.1,IEN,0),"^",7)
 . Q:X12CODE=""
 . S SAMEAS("URI")="HPTC:"_X12CODE
 . S SAMEAS("LABEL")=$P(^USC(8932.1,IEN,0),"^",2)
 ; For 61: SNOMED RT, snomed code and name
 Q
 ;
 ; Provider Narrative is used in problems (and POV, V CPT) to describe
 ; a problem. Most but not all resolve to expressions which in turn resolve
 ; to meaningful ICD codes.
RESVS9999999dot27(IEN,SAMEAS) ;
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^AUTNPOV(IEN,757))
 N SEVEN5701 S SEVEN5701=$P(^AUTNPOV(IEN,757),"^")
 Q:SEVEN5701=""
 D RESVS757dot01(SEVEN5701,.SAMEAS)
 Q  ; don't fall back on a 757.01 that doesn't resolve to 757
 ;
 ; Lexicon expressions: turn expression (757_01) into major concept (757)
RESVS757dot01(IEN,SAMEAS) ;
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^LEX(757.01,IEN,1))
 N SEVEN57 S SEVEN57=$P(^LEX(757.01,IEN,1),"^")
 Q:SEVEN57=""
 D RESVS757(SEVEN57,.SAMEAS)
 Q
 ;
RESVS757(IEN,SAMEAS) ;
 Q:'$D(^LEX(757,IEN,0))
 ; Even major concept has a major expression and its label comes from that
 N MJRE S MJRE=$P(^LEX(757,IEN,0),"^")
 Q:MJRE=""
 Q:'$D(^LEX(757.01,MJRE,0))
 N SAMEASLABEL S SAMEASLABEL=$P(^LEX(757.01,MJRE,0),"^")
 Q:SAMEASLABEL=""
 S SAMEAS("URI")="VA:757-"_IEN
 S SAMEAS("LABEL")=SAMEASLABEL
 Q
 ;
 ; Pharmacy Orderable Item facade for 50.
 ; Three cases: no link to 50, link to 50 but it doesn't link and 50 links.
 ; the second case leads to a qualified local ala "LOCAL:50-IEN"
RESVS50dot7(IEN,SAMEAS) ;
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^PSDRUG("ASP",IEN))
 N DRUGIEN S DRUGIEN=$O(^PSDRUG("ASP",IEN,""))
 D RESVS50(DRUGIEN,.SAMEAS)
 Q:SAMEAS("URI")'="LOCAL"
 N SAMEASLABEL S SAMEASLABEL=$P(^PSDRUG(DRUGIEN,0),"^")
 Q:SAMEASLABEL=""
 S SAMEAS("URI")="LOCAL:50-"_DRUGIEN
 S SAMEAS("LABEL")=SAMEASLABEL
 Q
 ;
 ; VistA Drug 50 to Standard 50.68 or mark as local
RESVS50(IEN,SAMEAS) ;
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^PSDRUG(IEN,"ND"))  ; Not mandatory to map to VA Product
 N VAPIEN S VAPIEN=$P(^PSDRUG(IEN,"ND"),"^",3)
 ; Q:VAPIEN'=+VAPIEN ; catch corrupt IEN
 Q:VAPIEN'=+VAPIEN  ; VAPIEN may be zero so can't be subscript
 D RESVSVUID("50.68",VAPIEN,$P(^PSDRUG(IEN,"ND"),"^",2),.SAMEAS)
 Q
 ;
 ;
 ; TBD: RESVS50_605 (DRUG CLASS). VA GETS hard codes a name map for this.
 ;
 ; TBD: one CPT resolver. Switch on 71, 790_2 and more. Merge the following
 ;
 ; Special: VistA Rad/Nuc Procedures 71 to Standard CPT
RESVS71(IEN,SAMEAS) ;
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^DIC(71,0,"GL"))
 N CODEAR S CODEAR=^DIC(71,0,"GL")
 D RESVSTOCPT(IEN,CODEAR,9,.SAMEAS)
 Q
 ;
 ; Special: VistA WV Procedures 790_2 to Standard CPT
RESVS790dot2(IEN,SAMEAS) ;
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^DIC(790.2,0,"GL"))
 N CODEAR S CODEAR=^DIC(790.2,0,"GL")
 D RESVSTOCPT(IEN,CODEAR,8,.SAMEAS)
 Q
 ;
 ; Reusable CPT sameas formatter
RESVSTOCPT(IEN,CODEAR,CPTFI,SAMEAS) ;
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(@(CODEAR_IEN_",0)"))
 N CPT S CPT=$P(@(CODEAR_IEN_",0)"),"^",CPTFI)
 Q:CPT=""
 Q:'$D(^ICPT("B",CPT))
 S CPTIEN=$O(^ICPT("B",CPT,""))
 N SAMEASLABEL S SAMEASLABEL=$P(^ICPT(CPTIEN,0),"^",2)
 Q:SAMEASLABEL=""
 S SAMEAS("URI")="CPT:"_CPT
 S SAMEAS("LABEL")=SAMEASLABEL
 Q
 ;
 ; TBD: LU LRVER1
RESVS60(IEN,SAMEAS) ;
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^LAB(60,IEN,64))
 ; Take Result NLT over National NLT
 N NLTIEN S NLTIEN=$S($P(^LAB(60,IEN,64),"^",2):$P(^LAB(60,IEN,64),"^",2),$P(^LAB(60,IEN,64),"^"):$P(^LAB(60,IEN,64),"^"),1:"")
 Q:NLTIEN=""
 Q:'$D(^LAM(NLTIEN))
 D RESVS64(NLTIEN,.SAMEAS)
 Q
 ;
 ; By design, do not map from WKLD to LOINC to its VUID. 
 ; VA assigned VUIDs to LOINCs but want LOINC and not VUID in sameas.
 ; TBD: LU LRVER1. See its logic.
RESVS64(IEN,SAMEAS) ;
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 N WKLDCODE S WKLDCODE=$P(^LAM(IEN,0),"^",2)
 Q:'WKLDCODE?5N1".0000"  ; leave local codes
 S SAMEAS("URI")="VA:wkld"_$P(WKLDCODE,".",1) ; 00000 dropped
 S SAMEAS("LABEL")=$P(^LAM(IEN,0),"^")
 Q:'$D(^LAM(IEN,9))
 N DEFLN S DEFLN=$P(^LAM(IEN,9),"^")
 Q:DEFLN=""
 Q:'$D(^LAB(95.3,DEFLN))
 Q:'$D(^LAB(95.3,DEFLN,81))  ; shortname
 S SAMEAS("URI")="LOINC:"_$P(^LAB(95.3,DEFLN,0),"^")_"-"_$P(^LAB(95.3,DEFLN,0),"^",15)  ; code and check_digit
 S SAMEAS("LABEL")=^LAB(95.3,DEFLN,81)
 Q
 ;
 ; Providers have NPIs
 ;
RESVS200(IEN,SAMEAS) ;
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^VA(200,IEN,"NPI"))
 N NPI S NPI=$P(^VA(200,IEN,"NPI"),"^")
 Q:NPI=""
 S SAMEAS("URI")="NPI:"_NPI
 S SAMEAS("LABEL")=$P(^VA(200,IEN,0),"^")
 Q
 ;
 ;
 ; C*** Resolution
 ;
RESC(FILENUM,IEN,DEFLABEL,SAMEAS) ; 
 Q:((FILENUM="")!(IEN=""))
 I FILENUM="50" D RESC50(IEN,.SAMEAS) Q  ; DRUG
 I FILENUM="8252" D RESC8252(IEN,.SAMEAS) Q  ; NDC
 I FILENUM="8250" D RESC8250(IEN,.SAMEAS) Q  ; INS/HICL
 I FILENUM="8250.1" D RESC8250P1(IEN,.SAMEAS) Q  ; IN/HIC
 ; TOADD: 8251 CDC - need to reach back to NDC for name
 I FILENUM="8254.01" D RESC8254P01(IEN,.SAMEAS) Q  ; All Sel
 I FILENUM="8188" D RESC8188(IEN,.SAMEAS) Q  ; LOINC
 D RESCSTDS(FILENUM,IEN,DEFLABEL,.SAMEAS) Q:$D(SAMEAS("URI"))
 Q
 ;
 ;
 ; Drug 50 to NDC or NDDF CDC or local
 ; Proxy for CDC 
 ;
RESC50(IEN,SAMEAS) ; nddf:cdc
 S SAMEAS("URI")="LOCAL"
 N PNDCIEN S PNDCIEN=$P(^PSDRUG(IEN,0),"^",4)
 Q:PNDCIEN=""
 D RESC8252(PNDCIEN,.SAMEAS)
 Q:SAMEAS("URI")="LOCAL"
 N GCNSEQNO S GCNSEQNO=$P(^PSDFDB(8252,PNDCIEN,0),"^")
 Q:GCNSEQNO'=+GCNSEQNO
 Q:GCNSEQNO=0
 ; Pad the id to 6 + cdc extension for NDDF
 S SAMEAS("URI")="NDDF:"_"cdc"_$TR($J(GCNSEQNO,6)," ","0")
 ; Leave SAMEAS LABEL == NDC Label
 Q
 ;
 ;
 ; NDC - separate from other standards as reused
 ; ... not reducing to nddf cdc
 ;     
RESC8252(IEN,SAMEAS) ; NDC
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^PSDFDB(8252,IEN,0))
 N NDCLBL S NDCLBL=$P(^PSDFDB(8252,IEN,0),"^",4)
 Q:NDCLBL=""
 ; NDC is IEN without -'s and leading 0's removed
 S SAMEAS("URI")="NDC:"_$TR($J(IEN,11)," ","0")
 S SAMEAS("LABEL")=NDCLBL
 Q
 ;
 ;
 ; INS (HICL) - 8250
 ;
RESC8250(IEN,SAMEAS) ; nddf:ins
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^PSDFDB(8250,IEN,0))
 N LBL S LBL=$P(^PSDFDB(8250,IEN,0),"^")
 Q:LBL=""
 ; HICL (seq no) is IEN padded ie/ leading 0's back
 S SAMEAS("URI")="NDDF:ins"_$TR($J(IEN,6)," ","0")
 S SAMEAS("LABEL")=LBL
 Q
 ; 
 ;
 ; IN (HIC) 8250.1
 ;
RESC8250P1(IEN,SAMEAS) ; nddf:in
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^PSDFDB(8250.1,IEN,0))
 N LBL S LBL=$P(^PSDFDB(8250.1,IEN,0),"^",2)
 Q:LBL=""
 ; IN is IEN padded ie/ leading 0's back
 S SAMEAS("URI")="NDDF:in"_$TR($J(IEN,6)," ","0")
 S SAMEAS("LABEL")=LBL
 Q
 ;
 ;
 ; Allergy Selection (8254.01)
 ; proxy for INS or DAC
 ;
 ; TOADD: selections that dress DACs 
 ;
RESC8254P01(IEN,SAMEAS) ; to nddf:ins
 ; No Q for LOCAL as valid to have no HICL
 Q:'$D(^PSDC(8254.01,IEN,0))
 N HICLIEN S HICLIEN=$P(^PSDC(8254.01,IEN,0),"^",3)
 ; Ex/ 8254_01-1160 (marked obsolete so!)
 Q:HICLIEN=""
 D RESC8250(HICLIEN,.SAMEAS)
 Q 
 ;
 ;
 ; LOINC - TOADD: link from 60
 ;
RESC8188(IEN,SAMEAS) ; LOINC
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^DALOINC(8188,IEN,0))
 Q:'$D(^DALOINC(8188,IEN,1))
 N CODE S CODE=$P(^DALOINC(8188,IEN,0),"^")
 Q:CODE=""
 N LBL S LBL=$P(^DALOINC(8188,IEN,1),"^",3)
 Q:LBL=""
 S SAMEAS("URI")="LOINC:"_CODE
 S SAMEAS("LABEL")=LBL
 Q
 ;
 ;
 ; Standard files: 80 (ICDCM Diag), 80.1 (ICDCM Proc)
 ; 
 ; TOADD: 8171 (HPTC), 8151 (cpt/hcpcs)
 ;
RESCSTDS(FILENUM,IEN,DEFLABEL,SAMEAS) ;
 ; Can't default to LOCAL as this is the default
 I FILENUM="80" D
 . Q:'$D(@("^ICD9("_IEN_",0)"))
 . S SAMEAS("URI")="ICD9CM:"_$P(DEFLABEL,"/",2)
 . S SAMEAS("LABEL")=$P(@("^ICD9("_IEN_",0)"),"^",3)
 I FILENUM="80.1" D
 . Q:'$D(@("^ICD0("_IEN_",0)"))
 . S SAMEAS("URI")="ICD9CM:"_$P(DEFLABEL,"/",2)
 . S SAMEAS("LABEL")=$P(@("^ICD0("_IEN_",0)"),"^",4)
 Q
 ;
