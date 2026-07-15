from math import floor


class BusinessRules:

    @staticmethod
    def to_be_accept(receiving, qty_receive_pending):
        receiving = max(0, float(receiving or 0))
        qty_receive_pending = max(0, float(qty_receive_pending or 0))
        return floor(min(qty_receive_pending, receiving))

    @staticmethod
    def dispatch_gap(inventory, active):
        inventory = max(0, float(inventory or 0))
        active = max(0, float(active or 0))
        return floor(max(0, active - inventory))

    @staticmethod
    def to_be_dispatch(inventory, active, dispatch_evidence):
        dispatch_evidence = max(0, float(dispatch_evidence or 0))
        gap = BusinessRules.dispatch_gap(inventory, active)
        return floor(min(gap, dispatch_evidence))
