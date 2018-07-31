#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Ingest script for CUAPS
   Programas Sociales
"""
import pandas as pd
import argparse
import pdb
def ingest_cuaps(update_date, output):

    data = pd.read_excel("s3://sedesol-lab/CUAPS-PROGRAMAS/CUAPS-Padrones-{0}.xlsx".\
            format(update_date), encoding="latin-1")
    data = data.fillna('')

    data["cve_padron"] = data.apply(lambda x: str(x["cve_padron"]).zfill(4),
            axis=1)

    data.to_csv(output, encoding="utf-8", sep="|", index=False)
    
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download Cuaps Dictionary')

    parser.add_argument('--start', type=str, default='2017-07',
                        help='Update year and month -  As string format yyyy-m')

    parser.add_argument('--output', type=str, help = 'Name of outputfile')
    args = parser.parse_args()
    start = args.start
    output = args.output

    ingest_cuaps(update_date=start, output=output)
