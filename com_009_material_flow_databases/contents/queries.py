FlowMFA = '''
SELECT c.Name AS Country, d.Country AS ISOAlpha3, f.Name AS Flow, m2.Name AS MFA13, m.Name AS MFA4, d.Year AS Year, d.Amount AS Amount
    FROM FlowMFA d LEFT JOIN Country c ON d.Country = c.Code
	LEFT JOIN Flow f ON d.Flow = f.Code
	LEFT JOIN MFA13 m2 ON d.MFA13 = m2.Code
	LEFT JOIN MFA4 m ON d.MFA4 = m.Code
	ORDER BY Flow, Year, MFA4, Country, MFA13;
  '''


FlowDetailed = '''
SELECT d.Year AS Year, c1.Name AS OriginCountry, d.Source AS OriginISOAlpha3, c2.Name AS ConsumerCountry, d.Destination AS ConsumerISOAlpha3, m.Name AS MFA4, p.Name AS ProductGroup, d.Amount AS Amount
	FROM FlowDetailed d LEFT JOIN Country c1 ON d.Source = c1.Code
	LEFT JOIN Country c2 ON d.Destination = c2.Code
	LEFT JOIN MFA4 m ON d.MFA4 = m.Code
	LEFT JOIN Productgroup p ON d.ProductGroup = p.Code
	ORDER BY Year, MFA4, ConsumerCountry, ProductGroup, OriginCountry;
  '''

Footprint = '''
SELECT d.Year AS Year, c2.Name AS ConsumerCountry, d.Destination AS ConsumerISOAlpha3, m.Name AS MFA4, sum(d.Amount) AS Amount
	FROM FlowDetailed d
	LEFT JOIN Country c2 ON d.Destination = c2.Code
	LEFT JOIN MFA4 m ON d.MFA4 = m.Code
	LEFT JOIN Productgroup p ON d.ProductGroup = p.Code
	GROUP BY Year, MFA4, ConsumerCountry
	ORDER BY Year, MFA4, ConsumerCountry;
  '''
