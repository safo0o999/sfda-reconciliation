from math import floor


class BusinessRules:
    @staticmethod
    def to_be_accept(receiving_packages, qty_receive_pending):
        receiving_packages = max(0, float(receiving_packages or 0))
        qty_receive_pending = max(0, float(qty_receive_pending or 0))
        return floor(min(qty_receive_pending, receiving_packages))

    @staticmethod
    def to_be_dispatch(active, dispatch_evidence_packages):
        active = max(0, float(active or 0))
        dispatch_evidence_packages = max(0, float(dispatch_evidence_packages or 0))
        return floor(min(active, dispatch_evidence_packages))
