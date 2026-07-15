import pandas as pd

from engine.grouper import Grouper
from config.business_rules import BusinessRules


class Calculator:

    KEYS = [
        "BN",
        "Expiry Date"
    ]

    DUMMY_CUSTOMER = "DUMMY CUSTOMER"
    DUMMY_GLN = "DUMMY"

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

        return (
            receiving,
            inventory,
            dispatch
        )

    @staticmethod
    def _apply_business_rules(row):

        to_be_accept = BusinessRules.to_be_accept(
            receiving=row["Receiving"],
            qty_receive_pending=row[
                "Quantity Receive Pending"
            ],
        )

        dispatch_requirement = (
            BusinessRules.dispatch_requirement(
                inventory=row["Inventory"],
                active=row["Active"],
            )
        )

        calculated_to_be_dispatch = (
            BusinessRules.to_be_dispatch(
                inventory=row["Inventory"],
                active=row["Active"],
                dispatch_evidence=row["Dispatch"],
            )
        )

        return pd.Series(
            {
                "To Be Accept": to_be_accept,
                "Dispatch Requirement":
                    dispatch_requirement,
                "Calculated To Be Dispatch":
                    calculated_to_be_dispatch,
                "Unexplained Dispatch Variance":
                    max(
                        0,
                        dispatch_requirement
                        - calculated_to_be_dispatch,
                    ),
            }
        )

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
    def _summarize_dispatch_customers(
        dispatch_df
    ):

        dispatch = dispatch_df.copy()

        dispatch["_Source Order"] = range(
            len(dispatch)
        )

        dispatch = dispatch[
            dispatch["Dispatched Quantity"] > 0
        ].copy()

        grouped = (
            dispatch
            .groupby(
                [
                    "BN",
                    "Expiry Date",
                    "To Address"
                ],
                as_index=False,
                dropna=False
            )
            .agg(
                {
                    "Dispatched Quantity": "sum",
                    "_Source Order": "min",
                    "Trade Item Number": "first",
                    "Trade Name": "first"
                }
            )
        )

        grouped["To Address"] = (
            grouped["To Address"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        grouped.loc[
            grouped["To Address"] == "",
            "To Address"
        ] = Calculator.DUMMY_CUSTOMER

        return grouped

    @staticmethod
    def _allocate_dispatch_by_customer(
        master,
        dispatch_df,
        gln_df
    ):

        customer_dispatch = (
            Calculator._summarize_dispatch_customers(
                dispatch_df
            )
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

        customer_dispatch = (
            customer_dispatch
            .merge(
                dispatch_targets,
                on=Calculator.KEYS,
                how="inner"
            )
        )

        customer_dispatch["PackageSize"] = (
            pd.to_numeric(
                customer_dispatch["PackageSize"],
                errors="coerce"
            )
            .fillna(1)
        )

        customer_dispatch.loc[
            customer_dispatch["PackageSize"] <= 0,
            "PackageSize"
        ] = 1

        customer_dispatch[
            "Actual Dispatch Packages"
        ] = (
            customer_dispatch[
                "Dispatched Quantity"
            ]
            / customer_dispatch["PackageSize"]
        )

        customer_dispatch[
            "Actual Dispatch Packages"
        ] = (
            customer_dispatch[
                "Actual Dispatch Packages"
            ]
            .fillna(0)
            .astype(float)
            .apply(int)
        )

        customer_dispatch = (
            customer_dispatch
            .sort_values(
                by=[
                    "BN",
                    "Expiry Date",
                    "_Source Order",
                    "To Address"
                ],
                kind="stable"
            )
            .reset_index(drop=True)
        )

        allocated_rows = []
        variance_rows = []

        for (
            batch_number,
            expiry_date
        ), group in customer_dispatch.groupby(
            Calculator.KEYS,
            sort=False,
            dropna=False
        ):

            target_quantity = int(
                group[
                    "Calculated To Be Dispatch"
                ].iloc[0]
            )

            remaining_quantity = (
                target_quantity
            )

            for _, row in group.iterrows():

                if remaining_quantity <= 0:
                    break

                actual_customer_quantity = int(
                    row[
                        "Actual Dispatch Packages"
                    ]
                )

                allocated_quantity = min(
                    remaining_quantity,
                    actual_customer_quantity
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

                remaining_quantity -= (
                    allocated_quantity
                )

            first_row = group.iloc[0]

            if remaining_quantity > 0:

                variance_rows.append(
                    {
                        "GTIN": first_row["GTIN"],
                        "Drug Name":
                            first_row["Drug Name"],
                        "BN": batch_number,
                        "Expiry Date": expiry_date,
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

        if allocated.empty:

            allocated = pd.DataFrame(
                columns=[
                    "GTIN",
                    "Drug Name",
                    "BN",
                    "Expiry Date",
                    "To Address",
                    "GLN",
                    "Customer Status",
                    "Trade Item Number",
                    "Trade Name",
                    "Dispatched Quantity",
                    "PackageSize",
                    "Actual Dispatch Packages",
                    "Calculated To Be Dispatch",
                    "Allocated To Be Dispatch",
                    "To Be Dispatch"
                ]
            )

        else:

            gln = Calculator._prepare_gln(
                gln_df
            )

            allocated = allocated.merge(
                gln,
                on="To Address",
                how="left"
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

            allocated.loc[
                ~registered_mask,
                "To Address"
            ] = Calculator.DUMMY_CUSTOMER

            allocated[
                "Allocated To Be Dispatch"
            ] = (
                allocated[
                    "Allocated To Be Dispatch"
                ]
                .fillna(0)
                .astype(int)
            )

            allocated[
                "To Be Dispatch"
            ] = (
                allocated[
                    "To Be Dispatch"
                ]
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
                        "Expiry Date"
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
            + variance["To Be Accept"]
            - variance[
                "Allocated To Be Dispatch"
            ]
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
            variance[
                "Dispatch Requirement"
            ]
            - variance[
                "Allocated To Be Dispatch"
            ]
        ).clip(lower=0)

        variance[
            "Missing Full Dispatch Evidence"
        ] = (
            variance[
                "Dispatch Requirement"
            ]
            - variance[
                "Calculated To Be Dispatch"
            ]
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

        master = sfda_df.copy()

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

        master["Dispatch Requirement"] = (
            master["Dispatch Requirement"]
            .fillna(0)
            .astype(int)
        )

        master[
            "Unexplained Dispatch Variance"
        ] = (
            master[
                "Unexplained Dispatch Variance"
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
