import sys


class load_commodity(object):
    def __init__(self, commodity_code, pref_2020, pref_2020_exclusions, pref_2027, pref_2005, pref_1032):
        self.commodity_code = commodity_code.strip()
        self.pref_2020 = pref_2020.strip()
        self.pref_2020_exclusions = pref_2020_exclusions.strip()
        self.pref_2027 = pref_2027.strip()
        self.pref_2005 = pref_2005.strip()
        self.pref_1032 = pref_1032.strip()

        self.get_exclusion_code()

    def get_exclusion_code(self):
        if self.pref_2020_exclusions == "":
            self.pref_2020_exclusions_code = ""
        elif self.pref_2020_exclusions == "Indonesia":
            self.pref_2020_exclusions_code = "ID"
        elif self.pref_2020_exclusions == "India":
            self.pref_2020_exclusions_code = "IN"
        elif self.pref_2020_exclusions == "Kenya":
            self.pref_2020_exclusions_code = "KE"
        else:
            print("Unknown exclusion code ", self.pref_2020_exclusions)
            sys.exit()
