<xsl:stylesheet version="3.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:xs="http://www.w3.org/2001/XMLSchema"
  exclude-result-prefixes="xs">

  <xsl:output method="xml" indent="yes"/>

  <xsl:template match="/">
    <parsed>
      <source_id>
        <xsl:value-of select="(/*/*[local-name()='FORM_SECTION']/*[local-name()='NOTICE_NUMBER'] | //*[local-name()='NOTICE_NUMBER'] | //*[local-name()='ID'] | //*[local-name()='NOTICE_ID'])[1]"/>
      </source_id>

      <title>
        <xsl:value-of select="(//*/text()[normalize-space()][ancestor::*[local-name()='TITLE']])[1]"/>
      </title>

      <description>
        <xsl:value-of select="(//*/text()[normalize-space()][ancestor::*[local-name()='SHORT_DESCRIPTION'] or ancestor::*[local-name()='DESCRIPTION']])[1]"/>
      </description>

      <buyer_name>
        <xsl:value-of select="(//*/text()[normalize-space()][ancestor::*[local-name()='BUYER_NAME'] or ancestor::*[local-name()='CONTRACTING_BODY']/*/*[local-name()='OFFICIALNAME']])[1]"/>
      </buyer_name>

      <buyer_country>
        <xsl:value-of select="(//*/text()[normalize-space()][ancestor::*[local-name()='COUNTRY']])[1]"/>
      </buyer_country>

      <cpv_codes>
        <xsl:for-each select="//*[local-name()='CPV_CODE' or local-name()='CPV']/*[local-name()='CODE'] | //*[local-name()='CPV_MAIN']/*[local-name()='CPV_CODE']/@CODE | //*[local-name()='CPV_ADDITIONAL']/*[local-name()='CPV_CODE']/@CODE | //@CPV_CODE">
          <code><xsl:value-of select="normalize-space(.)"/></code>
        </xsl:for-each>
      </cpv_codes>

      <published_at>
        <xsl:value-of select="(//*/text()[normalize-space()][ancestor::*[local-name()='DATE_PUBLICATION'] or ancestor::*[local-name()='DATE'] or ancestor::*[local-name()='PUBLICATION_DATE']])[1]"/>
      </published_at>

      <deadline>
        <xsl:value-of select="(//*/text()[normalize-space()][ancestor::*[local-name()='DATE_LIMIT'] or ancestor::*[local-name()='DEADLINE_TENDERS'] or ancestor::*[local-name()='DATE_RECEIPT_TENDERS']])[1]"/>
      </deadline>

      <url_notice>
        <xsl:value-of select="(//*/text()[normalize-space()][ancestor::*[local-name()='URI_DOC'] or ancestor::*[local-name()='NOTICE_URL']])[1]"/>
      </url_notice>

      <url_detail>
        <xsl:value-of select="(//*/text()[normalize-space()][ancestor::*[local-name()='URI_TED'] or ancestor::*[local-name()='URL']])[1]"/>
      </url_detail>

      <attachments>
        <xsl:for-each select="//*[local-name()='ATTACHMENT' or local-name()='DOCUMENT']">
          <a>
            <name><xsl:value-of select="(./*[local-name()='NAME']/text())[1]"/></name>
            <href><xsl:value-of select="(./*[local-name()='URL']/text() | ./@href | ./@url)[1]"/></href>
          </a>
        </xsl:for-each>
      </attachments>
    </parsed>
  </xsl:template>
</xsl:stylesheet>
