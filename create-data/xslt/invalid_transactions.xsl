<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:ed="http://www.eurodyn.com/Tariff/services/Taric3TransactionsLog/request/v03" version="2.0">
    <xsl:output method="text" encoding="utf-8"/>
    <xsl:strip-space elements="*"/>

    <xsl:template match="/">
		<xsl:text>Transaction ID</xsl:text><xsl:text>&#xd;</xsl:text>
        <xsl:for-each select="ed:taric3TransactionsLogData/ed:invalidTransactionData/ed:invalidTransaction">
            <xsl:value-of select="ed:transacationId"/>
            <xsl:text>&#xd;</xsl:text>
        </xsl:for-each>
    </xsl:template>
</xsl:stylesheet>

