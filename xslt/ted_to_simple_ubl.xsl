<xsl:stylesheet version="3.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:xs="http://www.w3.org/2001/XMLSchema"
  xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
  xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"
  xmlns:ext="urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2"
  xmlns:efext="http://data.europa.eu/p27/eforms-ubl-extensions/1"
  xmlns:efac="http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1"
  xmlns:efbc="http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1"
  xmlns:f="urn:local:fun"
  exclude-result-prefixes="xs cac cbc ext efext efac efbc f">

  <xsl:output method="xml" indent="yes"/>

  <!-- strip trailing +hh:mm from date/time -->
  <xsl:function name="f:clean" as="xs:string?">
    <xsl:param name="s" as="xs:string?"/>
    <xsl:sequence select="replace(normalize-space(string($s)), '\+\d{2}:\d{2}$', '')"/>
  </xsl:function>

  <!-- org lookup -->
  <xsl:key name="orgById" match="//efac:Organization/efac:Company"
           use="normalize-space(.//cac:PartyIdentification/cbc:ID[@schemeName='organization'])"/>

  <xsl:template match="/">
    <!-- Support both ContractNotice and ContractAwardNotice roots -->
    <xsl:variable name="root" select="/*[local-name() = ('ContractNotice','ContractAwardNotice')]"/>

    <!-- ids -->
    <xsl:variable name="pubId"    select="normalize-space((//efext:EformsExtension/efac:Publication/efbc:NoticePublicationID)[1])"/>
    <xsl:variable name="noticeId" select="normalize-space(($root/cbc:ID[@schemeName='notice-id'])[1])"/>

    <!-- buyer -->
    <xsl:variable name="buyerOrgId"   select="normalize-space(($root/cac:ContractingParty/cac:Party/cac:PartyIdentification/cbc:ID[@schemeName='organization'])[1])"/>
    <xsl:variable name="buyerCompany" select="key('orgById',$buyerOrgId)[1]"/>
    <xsl:variable name="firstCompany" select="(//efac:Organization/efac:Company)[1]"/>

    <!-- deadlines -->
    <xsl:variable name="dl_combined" select="(//efbc:SubmissionDeadline)[1]"/>
    <xsl:variable name="dl_date_raw" select="(//efbc:SubmissionDeadlineDate,
                                              //cac:TenderSubmissionDeadlinePeriod/cbc:EndDate,
                                              //cbc:SubmissionDueDate,
                                              //cbc:DueDate)[1]"/>
    <xsl:variable name="dl_time_raw" select="(//efbc:SubmissionDeadlineTime,
                                              //cac:TenderSubmissionDeadlinePeriod/cbc:EndTime,
                                              //cbc:SubmissionDueTime,
                                              //cbc:DueTime)[1]"/>
    <xsl:variable name="dl_date" select="f:clean($dl_date_raw)"/>
    <xsl:variable name="dl_time" select="f:clean($dl_time_raw)"/>

    <!-- robust title/description (CN + CAN) -->
    <xsl:variable name="title1" select="normalize-space(($root/cac:ProcurementProject/cbc:Name)[1])"/>
    <xsl:variable name="title2" select="normalize-space((//cac:ProcurementProjectLot/cac:TenderResult/cac:AwardedTenderedProject/cbc:Name)[1])"/>
    <xsl:variable name="title3" select="normalize-space((//cac:ProcurementProjectLot/cbc:Name)[1])"/>
    <xsl:variable name="desc1"  select="normalize-space(($root/cac:ProcurementProject/cbc:Description)[1])"/>
    <xsl:variable name="desc2"  select="normalize-space((//cac:ProcurementProjectLot/cbc:Description)[1])"/>
    <xsl:variable name="desc3"  select="normalize-space((//cbc:Description)[1])"/>

    <parsed>
      <source_id><xsl:value-of select="($pubId, $noticeId)[normalize-space()][1]"/></source_id>

      <!-- classification -->
      <notice_form><xsl:value-of select="local-name($root)"/></notice_form>
      <notice_type><xsl:value-of select="normalize-space((//cbc:NoticeTypeCode)[1])"/></notice_type>
      <notice_subtype><xsl:value-of select="normalize-space((//efac:NoticeSubType/cbc:SubTypeCode)[1])"/></notice_subtype>
      <procedure_code><xsl:value-of select="normalize-space(($root/cac:TenderingProcess/cbc:ProcedureCode)[1])"/></procedure_code>
      <notice_language><xsl:value-of select="normalize-space(($root/cbc:NoticeLanguageCode)[1])"/></notice_language>
      <regulatory_domain><xsl:value-of select="normalize-space(($root/cbc:RegulatoryDomain)[1])"/></regulatory_domain>

      <title><xsl:value-of select="($title1,$title2,$title3)[normalize-space()][1]"/></title>
      <description><xsl:value-of select="($desc1,$desc2,$desc3)[normalize-space()][1]"/></description>

      <buyer_name>
        <xsl:value-of select="normalize-space((($buyerCompany//cac:PartyName/cbc:Name)[1], ($firstCompany//cac:PartyName/cbc:Name)[1])[1])"/>
      </buyer_name>
      <buyer_country>
        <xsl:value-of select="normalize-space((($buyerCompany//cac:PostalAddress/cac:Country/cbc:IdentificationCode)[1], ($firstCompany//cac:PostalAddress/cac:Country/cbc:IdentificationCode)[1])[1])"/>
      </buyer_country>

      <cpv_codes>
        <xsl:for-each select="$root/cac:ProcurementProject/cac:MainCommodityClassification/cbc:ItemClassificationCode[@listName='cpv']">
          <code><xsl:value-of select="normalize-space(.)"/></code>
        </xsl:for-each>
        <xsl:for-each select="$root/cac:ProcurementProject/cac:AdditionalCommodityClassification/cbc:ItemClassificationCode[@listName='cpv']">
          <code><xsl:value-of select="normalize-space(.)"/></code>
        </xsl:for-each>
      </cpv_codes>

      <published_at>
        <xsl:value-of select="normalize-space(((//efext:EformsExtension/efac:Publication/efbc:PublicationDate)[1], concat(f:clean(($root/cbc:IssueDate)[1]),'T', f:clean(($root/cbc:IssueTime)[1])))[1])"/>
      </published_at>

      <deadline>
        <xsl:variable name="dt" select="if (normalize-space($dl_date) and normalize-space($dl_time)) then concat($dl_date,'T',$dl_time) else ''"/>
        <xsl:value-of select="normalize-space(($dt, $dl_combined, $dl_date)[normalize-space()][1])"/>
      </deadline>

      <!-- first URI across whole doc -->
      <url_notice><xsl:value-of select="normalize-space((//cac:CallForTendersDocumentReference/cac:Attachment/cac:ExternalReference/cbc:URI)[1])"/></url_notice>
      <url_detail/>

      <attachments>
        <xsl:for-each select="//cac:CallForTendersDocumentReference/cac:Attachment/cac:ExternalReference/cbc:URI">
          <a>
            <name><xsl:value-of select="normalize-space((../../cbc:DocumentType)[1])"/></name>
            <href><xsl:value-of select="normalize-space(.)"/></href>
          </a>
        </xsl:for-each>
      </attachments>
    </parsed>
  </xsl:template>
</xsl:stylesheet>
