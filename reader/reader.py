import re

from pyiceberg.table import StaticTable
from pyiceberg.io.fsspec import FsspecFileIO, FsspecInputFile, FsspecOutputFile, SCHEME_TO_FS, _adlfs
from pyiceberg.expressions import EqualTo

SCHEME_TO_FS["wasbs"] = _adlfs

def replace_wasbs(location: str):

    pattern = r'wasbs://([^@]+)@[^/]+(/.*)'
    replacement = r'abfs://\1\2'

    if location.startswith('wasbs://'):
        adsl_loc = re.sub(pattern, replacement, location)
        print(adsl_loc)
        return adsl_loc
    return location


class FsspecFileIOWrapper(FsspecFileIO):

    def _replace_wasbs(self, location: str):

        return replace_wasbs(location)
           
    def new_input(self, location: str) -> FsspecInputFile:

        location = self._replace_wasbs(location)
        return super().new_input(location)

    def new_output(self, location: str) -> FsspecOutputFile:

        location = self._replace_wasbs(location)
        return super().new_output(location)



if __name__=='__main__':


    static_table = StaticTable.from_metadata(
        "abfs://adres/adres/metadata/v<NUMBER>.metadata.json",
        properties={
            'py-io-impl': 'reader.FsspecFileIOWrapper',
            'adlfs.connection-string':'ADD THIS'
        }
    )

    table_scan = static_table.scan(
        selected_fields=("C_NATIONKEY",),
        )

    arrow_table = table_scan.to_pandas()

    print(arrow_table.drop_duplicates()['C_NATIONKEY'].to_list())

    keys = arrow_table.drop_duplicates()['C_NATIONKEY'].to_list()

    for partition in keys:
        part = static_table.scan(row_filter=EqualTo('C_NATIONKEY', partition)).to_pandas()
        print(f"data for key [ {partition} ]:\n\n")
        print(part)


