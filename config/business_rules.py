from math import floor


class BusinessRules:

    @staticmethod
    def to_be_accept(
        inventory,
        receiving,
        active,
        qty_sent_pending,
        qty_receive_pending
    ):

        inventory = max(0, inventory)
        receiving = max(0, receiving)
        active = max(0, active)
        qty_sent_pending = max(0, qty_sent_pending)
        qty_receive_pending = max(0, qty_receive_pending)

        if (
            inventory > active
            and qty_sent_pending == 0
            and qty_receive_pending > 0
        ):
            return floor(
                min(qty_receive_pending, inventory)
            )

        return floor(
            min(
                qty_receive_pending,
                receiving
            )
        )

    @staticmethod
    def to_be_dispatch(
        inventory,
        active
    ):

        inventory = max(0, inventory)
        active = max(0, active)

        if inventory == 0:
            return floor(active)

        return floor(
            max(
                0,
                active - inventory
            )
        )
