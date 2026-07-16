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
        """
        Calculate the quantity that can be uploaded to SFDA as Dispatch.

        Full Dispatch is the physical dispatch evidence. Inventory is not
        used to cap the upload quantity because its snapshot timing may be
        earlier than picking, staging, or dispatch confirmation.

        The upload quantity is capped only by:
        1. the current SFDA Active quantity; and
        2. the actual Full Dispatch quantity converted to packages.

        Inventory remains available separately for variance analysis.
        """
        active = max(0, float(active or 0))
        dispatch_evidence = max(
            0,
            float(dispatch_evidence or 0)
        )

        return floor(
            min(
                active,
                dispatch_evidence
            )
        )
