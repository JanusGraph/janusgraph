<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:pom="http://maven.apache.org/POM/4.0.0" exclude-result-prefixes="pom ">
    
    <xsl:output omit-xml-declaration="yes" />

    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()" />
        </xsl:copy>
    </xsl:template>

    <xsl:template match="pom:artifactId[../pom:groupId='com.thinkaurelius.titan' and contains(text(), '-jre6')]">
        <artifactId xmlns="http://maven.apache.org/POM/4.0.0"><xsl:value-of select="substring-before(./text(), '-jre6')" /></artifactId>
    </xsl:template>
    
    <!-- Rewrite <project><artifactId>...</artifactId></project> -->
    <xsl:template match="/pom:project/pom:artifactId[contains(text(), '-jre6')]">
        <artifactId xmlns="http://maven.apache.org/POM/4.0.0"><xsl:value-of select="substring-before(./text(), '-jre6')" /></artifactId>
    </xsl:template>
    
    <!-- Rewrite compiler target -->
    <xsl:template match="/pom:project/pom:properties/pom:compiler.target">
        <compiler.target>1.7</compiler.target>
    </xsl:template>
</xsl:stylesheet>
