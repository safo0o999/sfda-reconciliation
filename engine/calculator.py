import pandas as pd

from engine.grouper import Grouper
from config.business_rules import BusinessRules


class Calculator:

    KEYS = [
        "BN",
        "_EXPIRY_MONTH_KEY"
    ]

    DUMMY_CUSTOMER = "DUMMY CUSTOMER"
    DUMMY_GLN = "DUMMY"

    @staticmethod
    def _normalize_merge_keys(dataframe):
        """
        Normalize reconciliation matching keys.

        Matching uses:
            BN + Expiry Year/Month

        The complete Expiry Date remains unchanged for reports and the
        SFDA upload CSV. Only the internal matching key ignores the day.
        """
        if dataframe is None:
            return pd.DataFrame(
                columns=Calculator.KEYS
            )

        result = dataframe.copy()

        if "BN" not in result.columns:
            result["BN"] = ""

        if "Expiry Date" not in result.columns:
            result["Expiry Date"] = pd.NaT

        result["BN"] = (
            result["BN"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
            .str.replace(
                r"\.0$",
                "",
                regex=True
            )
        )

        result["Expiry Date"] = (
            pd.to_datetime(
                result["Expiry Date"],
                errors="coerce",
                dayfirst=True,
                format="mixed"
            )
            .dt.normalize()
            .astype("datetime64[ns]")
        )

        result["_EXPIRY_MONTH_KEY"] = (
            result["Expiry Date"]
            .dt.strftime("%Y-%m")
            .fillna("")
        )

        return result

    @staticmethod
    def _prepare_packsize(packsize_df):

        packsize = packsize_df[
            [
                "Trade Name",
                "PackageSize"
            ]
        ].copy()

        packsize = packsize[
            packsize["Trade Name"] != ""
        ]

        packsize["PackageSize"] = pd.to_numeric(
            packsize["PackageSize"],
            errors="coerce"
        ).fillna(1)

        packsize.loc[
            packsize["PackageSize"] <= 0,
            "PackageSize"
        ] = 1

        packsize = (
            packsize
            .sort_values(
                by="PackageSize",
                ascending=False
            )
            .drop_duplicates(
                subset=["Trade Name"],
                keep="first"
            )
        )

        return packsize.reset_index(
            drop=True
        )

    @staticmethod
    def _prepare_gln(gln_df):

        gln = gln_df[
            [
                "To Address",
                "GLN"
            ]
        ].copy()

        gln = gln[
            (gln["To Address"] != "")
            & (gln["GLN"] != "")
        ]

        gln = gln.drop_duplicates(
            subset=["To Address"],
            keep="first"
        )

        return gln.reset_index(
            drop=True
        )

    @staticmethod
    def _summarize_sources(
        receiving_df,
        inventory_df,
        dispatch_df
    ):

        receiving_df = Calculator._normalize_merge_keys(
            receiving_df
        )
        inventory_df = Calculator._normalize_merge_keys(
            inventory_df
        )
        dispatch_df = Calculator._normalize_merge_keys(
            dispatch_df
        )

        receiving = Grouper.summarize(
            receiving_df,
            Calculator.KEYS,
            "Received Quantity"
        )

        inventory = Grouper.summarize(
            inventory_df,
            Calculator.KEYS,
            "Available Quantity"
        )

        dispatch = Grouper.summarize(
            dispatch_df,
            Calculator.KEYS,
            "Dispatched Quantity"
        )

        # These summaries already contain the normalized matching keys.
        # Keep only keys + quantity so no full Expiry Date column can
        # collide with the SFDA master Expiry Date during merge.
        receiving = receiving[
            Calculator.KEYS
            + ["Received Quantity"]
        ].copy()

        inventory = inventory[
            Calculator.KEYS
            + ["Available Quantity"]
        ].copy()

        dispatch = dispatch[
            Calculator.KEYS
            + ["Dispatched Quantity"]
        ].copy()

        return (
            receiving,
            inventory,
            dispatch
        )

    @staticmethod
    def _apply_business_rules(row):

        to_be_accept = BusinessRules.to_be_accept(
            receiving=row["Receiving"],
            qty_receive_pending=row["Quantity Receive Pending"]
        )

        dispatch_gap = BusinessRules.dispatch_gap(
            inventory=row["Inventory"],
            active=row["Active"]
        )

        calculated_to_be_dispatch = BusinessRules.to_be_dispatch(
            inventory=row["Inventory"],
            active=row["Active"],
            dispatch_evidence=row["Dispatch"]
        )

        dispatch_evidence = max(
            0,
            float(row["Dispatch"] or 0)
        )

        active_quantity = max(
            0,
            float(row["Active"] or 0)
        )

        return pd.Series({
            "To Be Accept": to_be_accept,
            "Dispatch Gap": dispatch_gap,
            "Calculated To Be Dispatch":
                calculated_to_be_dispatch,
            "Unexplained Dispatch Variance": max(
                0,
                active_quantity
                - calculated_to_be_dispatch
            ) if dispatch_evidence > 0 else 0
        })

    @staticmethod
    def _build_accept(master):

        accept = master[
            master["To Be Accept"] > 0
        ][
            [
                "GTIN",
                "To Be Accept",
                "BN",
                "Expiry Date"
            ]
        ].copy()

        accept["To Be Accept"] = (
            accept["To Be Accept"]
            .fillna(0)
            .astype(int)
        )

        return accept.reset_index(
            drop=True
        )

    @staticmethod
    def _prepare_dispatch_rows(
        dispatch_df
    ):
        """
        Preserve every Full Dispatch transaction row.

        Allocation happens at Sales Order / Order Line level.
        Customer grouping is deferred until CSV export.
        """
        dispatch = Calculator._normalize_merge_keys(
            dispatch_df
        )

        dispatch["_Source Order"] = range(
            len(dispatch)
        )

        if "Dispatched Quantity" not in dispatch.columns:
            dispatch["Dispatched Quantity"] = 0

        dispatch["Dispatched Quantity"] = pd.to_numeric(
            dispatch["Dispatched Quantity"],
            errors="coerce"
        ).fillna(0)

        optional_columns = {
            "To Address": Calculator.DUMMY_CUSTOMER,
            "Sales Order Number": "",
            "Order Line": "",
            "Trade Item Number": "",
            "Trade Name": "",
            "Generic Item Number": "",
            "Confirm Date": pd.NaT,
            "Order Line Status": "",
        }

        for column, default in optional_columns.items():
            if column not in dispatch.columns:
                dispatch[column] = default

        dispatch["To Address"] = (
            dispatch["To Address"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        dispatch.loc[
            dispatch["To Address"] == "",
            "To Address"
        ] = Calculator.DUMMY_CUSTOMER

        dispatch = dispatch[
            dispatch["Dispatched Quantity"] > 0
        ].copy()

        # Preserve the WMS date for diagnostics and Dispatch Details.
        # The merged Expiry Date will come from the SFDA master.
        dispatch["WMS Expiry Date"] = (
            dispatch["Expiry Date"]
        )

        dispatch = dispatch.drop(
            columns=["Expiry Date"]
        )

        return dispatch.reset_index(
            drop=True
        )

    @staticmethod
    def _allocate_dispatch_by_customer(
        master,
        dispatch_df,
        gln_df
    ):
        """
        Allocate the batch target across original Full Dispatch rows.

        Every Sales Order and Order Line remains available in the
        allocation output. CSV customer grouping happens later.
        """
        master = Calculator._normalize_merge_keys(
            master
        )

        dispatch_rows = Calculator._prepare_dispatch_rows(
            dispatch_df
        )

        dispatch_targets = master[
            master[
                "Calculated To Be Dispatch"
            ] > 0
        ][
            [
                "GTIN",
                "Drug Name",
                "BN",
                "Expiry Date",
                "PackageSize",
                "Calculated To Be Dispatch"
            ]
        ].copy()

        dispatch_targets = Calculator._normalize_merge_keys(
            dispatch_targets
        )

        transaction_dispatch = dispatch_rows.merge(
            dispatch_targets,
            on=Calculator.KEYS,
            how="inner",
            validate="many_to_one"
        )

        transaction_dispatch["PackageSize"] = (
            pd.to_numeric(
                transaction_dispatch["PackageSize"],
                errors="coerce"
            )
            .fillna(1)
        )

        transaction_dispatch.loc[
            transaction_dispatch["PackageSize"] <= 0,
            "PackageSize"
        ] = 1

        transaction_dispatch[
            "Actual Dispatch Packages"
        ] = (
            pd.to_numeric(
                transaction_dispatch[
                    "Dispatched Quantity"
                ],
                errors="coerce"
            ).fillna(0)
            / transaction_dispatch["PackageSize"]
        )

        transaction_dispatch[
            "Actual Dispatch Packages"
        ] = (
            transaction_dispatch[
                "Actual Dispatch Packages"
            ]
            .fillna(0)
            .astype(float)
            .apply(int)
        )

        transaction_dispatch = (
            transaction_dispatch
            .sort_values(
                by=[
                    "BN",
                    "Expiry Date",
                    "_Source Order"
                ],
                kind="stable"
            )
            .reset_index(drop=True)
        )

        allocated_rows = []
        variance_rows = []

        for (
            batch_number,
            expiry_month_key
        ), group in transaction_dispatch.groupby(
            Calculator.KEYS,
            sort=False,
            dropna=False
        ):
            target_quantity = int(
                group[
                    "Calculated To Be Dispatch"
                ].iloc[0]
            )

            remaining_quantity = target_quantity

            for _, row in group.iterrows():
                if remaining_quantity <= 0:
                    break

                actual_row_quantity = int(
                    row[
                        "Actual Dispatch Packages"
                    ]
                )

                allocated_quantity = min(
                    remaining_quantity,
                    actual_row_quantity
                )

                if allocated_quantity <= 0:
                    continue

                allocated_row = row.to_dict()
                allocated_row[
                    "Allocated To Be Dispatch"
                ] = allocated_quantity
                allocated_row[
                    "To Be Dispatch"
                ] = allocated_quantity

                allocated_rows.append(
                    allocated_row
                )

                remaining_quantity -= allocated_quantity

            first_row = group.iloc[0]

            if remaining_quantity > 0:
                variance_rows.append(
                    {
                        "GTIN": first_row["GTIN"],
                        "Drug Name":
                            first_row["Drug Name"],
                        "BN": batch_number,
                        "Expiry Date":
                            first_row["Expiry Date"],
                        "Expiry Match Month":
                            expiry_month_key,
                        "To Address": "N/A",
                        "GLN": "N/A",
                        "Customer Status":
                            "UNALLOCATED",
                        "Calculated To Be Dispatch":
                            target_quantity,
                        "Allocated To Be Dispatch":
                            target_quantity
                            - remaining_quantity,
                        "Remaining To Be Dispatch":
                            remaining_quantity,
                        "Variance Type":
                            "MISSING DISPATCH EVIDENCE"
                    }
                )

        allocated = pd.DataFrame(
            allocated_rows
        )

        allocated_columns = [
            "GTIN",
            "Drug Name",
            "BN",
            "Expiry Date",
            "To Address",
            "GLN",
            "Customer Status",
            "Sales Order Number",
            "Order Line",
            "Trade Item Number",
            "Trade Name",
            "Generic Item Number",
            "Confirm Date",
            "Order Line Status",
            "Dispatched Quantity",
            "PackageSize",
            "Actual Dispatch Packages",
            "Calculated To Be Dispatch",
            "Allocated To Be Dispatch",
            "To Be Dispatch",
            "_Source Order",
        ]

        if allocated.empty:
            allocated = pd.DataFrame(
                columns=allocated_columns
            )
        else:
            gln = Calculator._prepare_gln(
                gln_df
            )

            allocated = allocated.merge(
                gln,
                on="To Address",
                how="left",
                validate="many_to_one"
            )

            allocated[
                "Original To Address"
            ] = allocated["To Address"]

            registered_mask = (
                allocated["GLN"]
                .notna()
                & (
                    allocated["GLN"]
                    .astype(str)
                    .str.strip()
                    != ""
                )
            )

            allocated[
                "Customer Status"
            ] = "DUMMY"

            allocated.loc[
                registered_mask,
                "Customer Status"
            ] = "REGISTERED"

            allocated.loc[
                ~registered_mask,
                "GLN"
            ] = Calculator.DUMMY_GLN

            for quantity_column in [
                "Allocated To Be Dispatch",
                "To Be Dispatch",
                "Actual Dispatch Packages"
            ]:
                allocated[quantity_column] = (
                    pd.to_numeric(
                        allocated[quantity_column],
                        errors="coerce"
                    )
                    .fillna(0)
                    .astype(int)
                )

            allocated = (
                allocated
                .sort_values(
                    by=[
                        "Customer Status",
                        "To Address",
                        "GTIN",
                        "BN",
                        "Expiry Date",
                        "_Source Order"
                    ],
                    kind="stable"
                )
                .reset_index(drop=True)
            )

        allocation_variance = pd.DataFrame(
            variance_rows
        )

        return (
            allocated,
            allocation_variance
        )

    @staticmethod
    def _build_master_variance(master):

        variance = master.copy()

        variance["Allocated To Be Dispatch"] = (
            variance[
                "Allocated To Be Dispatch"
            ]
            .fillna(0)
        )

        variance["New Active"] = (
            variance["Active"]
            - variance["Allocated To Be Dispatch"]
        )

        variance[
            "Remaining Receive Pending"
        ] = (
            variance[
                "Quantity Receive Pending"
            ]
            - variance["To Be Accept"]
        ).clip(lower=0)

        variance[
            "Remaining Sent Pending"
        ] = (
            variance[
                "Quantity sent pending"
            ]
            - variance[
                "Allocated To Be Dispatch"
            ]
        ).clip(lower=0)

        variance["Active Variance"] = (
            variance["New Active"]
            - variance["Inventory"]
        )

        variance["Receive Variance"] = (
            variance[
                "Remaining Receive Pending"
            ]
            - (
                variance["Receiving"]
                - variance["To Be Accept"]
            ).clip(lower=0)
        )

        variance["Dispatch Variance"] = (
            variance["Dispatch Gap"]
            - variance["Allocated To Be Dispatch"]
        ).clip(lower=0)

        variance = variance[
            (variance["Active Variance"] != 0)
            | (variance["Receive Variance"] != 0)
            | (variance["Dispatch Variance"] != 0)
        ].copy()

        variance["Variance Type"] = (
            "RECONCILIATION VARIANCE"
        )

        return variance

    @staticmethod
    def calculate(
        sfda_df,
        receiving_df,
        inventory_df,
        dispatch_df,
        packsize_df,
        gln_df
    ):

        sfda_df = Calculator._normalize_merge_keys(
            sfda_df
        )
        receiving_df = Calculator._normalize_merge_keys(
            receiving_df
        )
        inventory_df = Calculator._normalize_merge_keys(
            inventory_df
        )
        dispatch_df = Calculator._normalize_merge_keys(
            dispatch_df
        )

        packsize = (
            Calculator._prepare_packsize(
                packsize_df
            )
        )

        (
            receiving,
            inventory,
            dispatch_summary
        ) = Calculator._summarize_sources(
            receiving_df,
            inventory_df,
            dispatch_df
        )

        master = Calculator._normalize_merge_keys(
            sfda_df
        )

        # Source summaries already have BN + expiry-month keys and do not
        # contain a full Expiry Date column.
        master = master.merge(
            packsize,
            left_on="Drug Name",
            right_on="Trade Name",
            how="left"
        )

        master["PackageSize"] = (
            pd.to_numeric(
                master["PackageSize"],
                errors="coerce"
            )
            .fillna(1)
        )

        master.loc[
            master["PackageSize"] <= 0,
            "PackageSize"
        ] = 1

        master = master.merge(
            receiving,
            on=Calculator.KEYS,
            how="left"
        )

        master = master.merge(
            inventory,
            on=Calculator.KEYS,
            how="left"
        )

        master = master.merge(
            dispatch_summary,
            on=Calculator.KEYS,
            how="left"
        )

        quantity_columns = [
            "Received Quantity",
            "Available Quantity",
            "Dispatched Quantity"
        ]

        for column in quantity_columns:

            master[column] = pd.to_numeric(
                master[column],
                errors="coerce"
            ).fillna(0)

        master["Receiving"] = (
            master["Received Quantity"]
            / master["PackageSize"]
        ).fillna(0)

        master["Inventory"] = (
            master["Available Quantity"]
            / master["PackageSize"]
        ).fillna(0)

        master["Dispatch"] = (
            master["Dispatched Quantity"]
            / master["PackageSize"]
        ).fillna(0)

        calculated = master.apply(
            Calculator._apply_business_rules,
            axis=1
        )

        master = pd.concat(
            [
                master,
                calculated
            ],
            axis=1
        )

        master["To Be Accept"] = (
            master["To Be Accept"]
            .fillna(0)
            .astype(int)
        )

        master[
            "Calculated To Be Dispatch"
        ] = (
            master[
                "Calculated To Be Dispatch"
            ]
            .fillna(0)
            .astype(int)
        )

        (
            dispatch_allocated,
            allocation_variance
        ) = (
            Calculator
            ._allocate_dispatch_by_customer(
                master=master,
                dispatch_df=dispatch_df,
                gln_df=gln_df
            )
        )

        allocated_summary = (
            dispatch_allocated
            .groupby(
                Calculator.KEYS,
                as_index=False,
                dropna=False
            )[
                "Allocated To Be Dispatch"
            ]
            .sum()
        )

        allocated_summary = allocated_summary[
            Calculator.KEYS
            + ["Allocated To Be Dispatch"]
        ].copy()

        master = Calculator._normalize_merge_keys(
            master
        )

        master = master.merge(
            allocated_summary,
            on=Calculator.KEYS,
            how="left"
        )

        master[
            "Allocated To Be Dispatch"
        ] = (
            master[
                "Allocated To Be Dispatch"
            ]
            .fillna(0)
            .astype(int)
        )

        master[
            "Remaining To Be Dispatch"
        ] = (
            master[
                "Calculated To Be Dispatch"
            ]
            - master[
                "Allocated To Be Dispatch"
            ]
        ).clip(lower=0)

        accept = Calculator._build_accept(
            master
        )

        master_variance = (
            Calculator._build_master_variance(
                master
            )
        )

        if allocation_variance.empty:

            variance = master_variance

        else:

            variance = pd.concat(
                [
                    master_variance,
                    allocation_variance
                ],
                ignore_index=True,
                sort=False
            )

        return {
            "master": master,
            "accept": accept,
            "dispatch": dispatch_allocated,
            "variance": variance
        }
