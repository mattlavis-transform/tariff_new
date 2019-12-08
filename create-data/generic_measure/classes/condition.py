
class condition(object):
    def __init__(self, condition_code, action_code, certificate_type_code, certificate_code):
        self.condition_code = condition_code
        self.action_code = action_code
        self.certificate_type_code = certificate_type_code
        self.certificate_code = certificate_code

    def xml(self):
        pass
