FMQLSMAC; Caregraf - FMQL Schema Enhancement for Terminologies (Other) ; Apr, 2013
 ;;v1.0b;FMQLQP;;Apr 29th, 2013
 ;
 ;
 ; FMQL Schema Enhancement for Terminologies - sameas relationships
 ;
 ; FMQL Query Processor (c) Caregraf 2010-2013 AGPL
 ;
 ; Link (declare sameas) local vocabulary to standard or national equivalents. 
 ; This is part of FMQL's FileMan schema enhancements and all vocabs identified
 ; in the vocabulary graph should be processed here.
 ;
 ; SAMEAS for Suite C
 ; 
RESOLVE(IEN,SAMEAS)
 I FILENUM="50" D RESOLVE50(IEN,.SAMEAS) Q ; DRUG
 Q
 ;
 ;
RESOLVE50(IEN,SAMEAS) ; VistA Drug 50 to NDC or NDDF equivalent or mark as local
 S:'$D(SAMEAS("URI")) SAMEAS("URI")="LOCAL"
 Q:'$D(^PSDRUG(IEN,"ND")) ; Should be but not presuming mandatory to map to primary NDC
 N PNDCIEN S PNDCIEN=$P(^PSDRUG(IEN,0),"^",4)
 Q:PNDCIEN'=+PNDCIEN
 Q:PNDCIEN=0 ; catch corrupt IEN
 Q:'$D(^PSDFDB(8252,PNDCIEN,0))
 ; TODO: pad the NDC as fallback sameas
 N GCNSEQNO S GCNSEQNO=$P(^PSDFDB(8252,PNDCIEN,0),"^")
 Q:GCNSEQNO'=+GCNSEQNO
 Q:GCNSEQNO=0
 ; Pad the id to 6 + cdc extension for NDDF
 N CDCID S CDCID="cdc"_$TR($J(GCNSEQNO,6)," ","0")
 S SAMEAS("URI")="nddf:"_CDCID
 S SAMEAS("LABEL")="TODO" ; TODO: label - doesn't seem to always or ever have itself. Get from MIN or leave?
 Q