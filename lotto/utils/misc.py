import logging


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def transfer_config(external_config):
    def rec_d(conf):
        d = {}
        fields = sorted([e for e in dir(conf)
                         if (not e[0] == '_') and (not e == "count")
                         and (not e == "index")])
        # logging.info(f"{dir(conf)}")
        for field in fields:
            attr = getattr(conf, field)
            if isinstance(attr, str) or isinstance(attr, int) \
                    or isinstance(attr, float):
                d[field] = attr
            elif isinstance(attr, list):  # this is critical! error still otherwise
                d[field] = []
                for a in attr:
                    d[field].append(rec_d(a))
            else:
                d[field] = rec_d(attr)
        return AttrDict(d)

    result = rec_d(external_config)
    # logging.info(f"[transfer_config] {result}")
    return result
