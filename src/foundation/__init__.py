__version__ = "0.0.1"
from .__main__ import remake
from .common import (
    add_to,
    bulk_update,
    env,
    normalize_geo_name,
    prettify_sql,
    run_sql_file,
)
from .loaders.enrollment import set_enrollment_tables
from .plugins.geodata import set_coordinates
from .plugins.matching import match_psgc_schools
from .plugins.matching.barangay import (
    apply_barangay_corrections,
    attach_psgc_brgy_id,
)
from .plugins.matching.municipality import attach_psgc_muni_id
from .plugins.matching.province import attach_psgc_provhuc_codes
from .plugins.matching.region import attach_psgc_region_codes
from .plugins.meta import unpack_enroll_data
from .plugins.psgc import set_psgc
from .transforms.fixes import fill_missing_psgc
from .transforms.reorder import reorganize_school_geo_df
