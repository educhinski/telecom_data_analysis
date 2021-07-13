import numpy as np
import pandas as pd
from helper import TelecomHelper
class CleanTelecomData:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')

    def drop_duplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        drop duplicate rows
        """
        df.drop_duplicates(inplace=True)

        return df

    def convert_to_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        convert column to datetime
        """

        df['start'] = pd.to_datetime(
            df['start'])
        df['end'] = pd.to_datetime(
            df['end'])

        return df

    def drop_columns_with_null_values(self, df: pd.DataFrame, threshold_in_percent=30) -> pd.DataFrame:
        Helper = TelecomHelper()
        
        overall_missing = Helper.percent_missing(self.df)

        null_percent_df = pd.DataFrame(columns=['column', 'null_percent'])
        columns = df.columns.values.tolist()

        null_percent_df['column'] = columns
        null_percent_df['null_percent'] = null_percent_df['column'].map(
            lambda x: Helper.percent_missing_for_col(df, x))
        
        columns_to_be_dropped = null_percent_df[null_percent_df['null_percent']
                                                > threshold_in_percent]['column'].to_list()
        df = self.__drop_columns(df, columns_to_be_dropped)

        return df

    def convert_to_mega_bytes(self, df):

        df = self.__convert_bytes_to_megabytes(df, 'social_media_dl_(bytes)')
        df = self.__convert_bytes_to_megabytes(df, 'social_media_ul_(bytes)')

        df = self.__convert_bytes_to_megabytes(df, "google_dl_(bytes)")
        df = self.__convert_bytes_to_megabytes(df, "google_ul_(bytes)")

        df = self.__convert_bytes_to_megabytes(df, "email_dl_(bytes)")
        df = self.__convert_bytes_to_megabytes(df, "email_ul_(bytes)")

        df = self.__convert_bytes_to_megabytes(df, "youtube_dl_(bytes)")
        df = self.__convert_bytes_to_megabytes(df, "youtube_ul_(bytes)")

        df = self.__convert_bytes_to_megabytes(df, "netflix_dl_(bytes)")
        df = self.__convert_bytes_to_megabytes(df, "netflix_ul_(bytes)")

        df = self.__convert_bytes_to_megabytes(df, "gaming_dl_(bytes)")
        df = self.__convert_bytes_to_megabytes(df, "gaming_ul_(bytes)")

        df = self.__convert_bytes_to_megabytes(df, "other_dl_(bytes)")
        df = self.__convert_bytes_to_megabytes(df, "other_ul_(bytes)")

        df = self.__convert_bytes_to_megabytes(df, "total_dl_(bytes)")
        df = self.__convert_bytes_to_megabytes(df, "total_ul_(bytes)")

        converted_df = df.rename(columns={'social_media_dl_(bytes)': 'social_media_dl',
                                          'social_media_ul_(bytes)': 'social_media_ul',

                                          'google_dl_(bytes)': 'google_dl',
                                          'google_ul_(bytes)': 'google_ul',

                                          'email_dl_(bytes)': 'email_dl',
                                          'email_ul_(bytes)': 'email_ul',

                                          'youtube_dl_(bytes)': 'youtube_dl',
                                          'youtube_ul_(bytes)': 'youtube_ul',

                                          'netflix_dl_(bytes)': 'netflix_dl',
                                          'netflix_ul_(bytes)': 'netflix_ul',

                                          'gaming_dl_(bytes)': 'gaming_dl',
                                          'gaming_ul_(bytes)': 'gaming_ul',

                                          'other_dl_(bytes)': 'other_dl',
                                          'other_ul_(bytes)': 'other_ul',

                                          'total_dl_(bytes)': 'total_dl',
                                          'total_ul_(bytes)': 'total_ul',
                                          })
        return converted_df

    def __drop_columns(self, df, columns=[]):

        return df.drop(columns, axis=1)

    def __convert_bytes_to_megabytes(df, bytes_data):

        megabyte = 1*10e+5
        megabyte_col = df[bytes_data] / megabyte

        return megabyte_col
