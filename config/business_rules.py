from math import floor


class BusinessRules:

    @staticmethod
    def to_be_accept(
        receiving,
        qty_receive_pending,
        **_,
    ):
        """
        Accept only quantities supported by actual ASN receipt evidence.
        Inventory and Active do not increase the Accept quantity.
        """
        receiving = max(0, float(receiving or 0))
        qty_receive_pending = max(
            0,
            float(qty_receive_pending or 0),
        )

        return floor(
            min(
                qty_receive_pending,
                receiving,
            )
        )

    @staticmethod
    def dispatch_requirement(
        inventory,
        active,
    ):
        """Quantity missing from inventory compared with SFDA Active."""
        inventory = max(0, float(inventory or 0))
        active = max(0, float(active or 0))

        return floor(
            max(
                0,
                active - inventory,
            )
        )

    @staticmethod
    def to_be_dispatch(
        inventory,
        active,
        dispatch_evidence,
    ):
        """
        Dispatch only the quantity supported by Full Dispatch evidence.
        Any remaining Active-versus-Inventory gap stays in Variance.
        """
        dispatch_evidence = max(
            0,
            float(dispatch_evidence or 0),
        )

        requirement = BusinessRules.dispatch_requirement(
            inventory=inventory,
            active=active,
        )

        return floor(
            min(
                requirement,
                dispatch_evidence,
            )
        )
