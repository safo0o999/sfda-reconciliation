from math import floor


class BusinessRules:
    @staticmethod
    def _value(value):
        try:
            return max(0.0, float(value or 0))
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def to_be_accept(
        receiving_packages,
        inventory_packages,
        active,
        qty_send_pending,
        qty_receive_pending,
    ):
        receiving = BusinessRules._value(receiving_packages)
        inventory = BusinessRules._value(inventory_packages)
        active = BusinessRules._value(active)
        send_pending = BusinessRules._value(qty_send_pending)
        receive_pending = BusinessRules._value(qty_receive_pending)

        if (
            inventory > active
            and send_pending == 0
            and receive_pending > 0
        ):
            return floor(min(receive_pending, inventory))

        return floor(min(receive_pending, receiving))

    @staticmethod
    def to_be_dispatch(active, inventory_packages):
        active = BusinessRules._value(active)
        inventory = BusinessRules._value(inventory_packages)

        if inventory <= 0:
            return floor(active)

        return floor(max(0, active - inventory))
