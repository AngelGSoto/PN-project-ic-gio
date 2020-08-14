from pyvo.dal import TAPService
import astropy
from astropy.table import Table, vstack
import sys

tap = TAPService("http://tap.roe.ac.uk/osa/")
maxPrevSourceID = 0
merged_table = None
merged_table_list = []
chunksize = 1000
maxcount = 1000000
count = 0

while count < maxcount:
    query_text = f"SELECT top {chunksize} * FROM VPHASDR3.vphasSource WHERE r_2AperMag6 < 19.5 AND iAperMag6Err <=0.1 AND r_2AperMag6Err <=0.1 AND havphasAperMag6Err <=0.1 AND (r_2AperMag6 - havphasAperMag6) > 0.25*(r_2AperMag6 - iAperMag6) + 1.9 AND iAperMag6 >0 and havphasAperMag6 >0 AND sourceID > {maxPrevSourceID} order by sourceID ASC"
    results = tap.search(query_text)

    res_table = results.resultstable.to_table(use_names_over_ids=True)

    if len(res_table) == 0:
        break

    merged_table_list.append(res_table)
    maxPrevSourceID = res_table[-1]["sourceID"]

    if len(res_table) < chunksize:
        break

    count += 1

merged_table = vstack (merged_table_list)

merged_table.write("votable.xml", format="votable")
