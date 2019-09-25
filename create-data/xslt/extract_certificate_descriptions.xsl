<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:xd="http://www.oxygenxml.com/ns/doc/xsl"
    xmlns:oub="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0"
    xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0"
    exclude-result-prefixes="xs xd"
    version="2.0">
    <xsl:output method="text" encoding="utf-8"/>
    <xsl:strip-space elements="*"/>
    <xd:doc scope="stylesheet">
        <xd:desc>
            <xd:p><xd:b>Created on:</xd:b> Feb 24, 2019</xd:p>
            <xd:p><xd:b>Author:</xd:b> Matt.Lavis</xd:p>
            <xd:p></xd:p>
        </xd:desc>
    </xd:doc>
    
    
    <xsl:template match ="/">
        
        <xsl:text>&quot;certificate.type.code&quot;,</xsl:text>
        <xsl:text>&quot;certificate.code&quot;,</xsl:text>
        <xsl:text>&quot;description&quot;,</xsl:text>
        <xsl:text>&quot;update.type&quot;</xsl:text>
        <xsl:text>&#xd;</xsl:text>
        <xsl:for-each select="//oub:certificate.description">
            <xsl:text>&quot;</xsl:text><xsl:value-of select="oub:certificate.type.code"/><xsl:text>&quot;,</xsl:text>
            <xsl:text>&quot;</xsl:text><xsl:value-of select="oub:certificate.code"/><xsl:text>&quot;,</xsl:text>
            <xsl:text>&quot;</xsl:text><xsl:value-of select="oub:description"/><xsl:text>&quot;,</xsl:text>
            <xsl:text>&quot;</xsl:text><xsl:value-of select="../oub:update.type"/><xsl:text>&quot;</xsl:text>
            <xsl:text>&#xd;</xsl:text>
        </xsl:for-each>

    </xsl:template>
</xsl:stylesheet>
