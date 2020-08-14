from pyvo.dal import TAPService
import astropy
from astropy.table import Table, vstack
import sys

tap_service = TAPService("http://dc.g-vo.org/tap")
tap_results = tap_service.search("SELECT TOP 10 * FROM ivoa.obscore")
print(tap_results)
