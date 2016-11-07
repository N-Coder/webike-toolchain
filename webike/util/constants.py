from datetime import datetime, timedelta

IMEIS = ['0587', '0603', '0636', '0657', '0665', '0669', '1210', '1473', '2910', '3014', '3215', '3410', '3469', '4381',
         '5233', '5432', '6089', '6097', '6473', '6904', '6994', '7303', '7459', '7517', '7710', '8508', '8664', '8870',
         '9050', '9399', '9407', '9519']
# TODO participant information

STUDY_START = datetime(year=2014, month=1, day=1)
TD0 = timedelta(0)


def discharge_curr_to_ampere(val):
    """Convert DischargeCurr from the DB from the raw sensor value to amperes"""
    return (val - 504) * 0.033 if val else 0
