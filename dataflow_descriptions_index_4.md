```
DataflowDescription {
    source_imports: {},
    index_imports: {
        User(
            15,
        ): IndexImport {
            desc: IndexDesc {
                on_id: User(
                    5,
                ),
                key: [
                    Column(
                        1,
                    ),
                ],
            },
            typ: RelationType {
                column_types: [
                    ColumnType {
                        scalar_type: Int64,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: String,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: String,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: Int64,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: String,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: Numeric {
                            max_scale: Some(
                                NumericMaxScale(
                                    2,
                                ),
                            ),
                        },
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: String,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: String,
                        nullable: false,
                    },
                ],
                keys: [
                    [
                        0,
                    ],
                ],
            },
            monotonic: false,
            usage_types: Some(
                [
                    FullScan,
                    IndexExport,
                ],
            ),
        },
    },
    objects_to_build: [
        BuildDesc {
            id: User(
                16,
            ),
            plan: OptimizedMirRelationExpr(
                ArrangeBy {
                    input: Get {
                        id: Global(
                            User(
                                5,
                            ),
                        ),
                        typ: RelationType {
                            column_types: [
                                ColumnType {
                                    scalar_type: Int64,
                                    nullable: false,
                                },
                                ColumnType {
                                    scalar_type: String,
                                    nullable: false,
                                },
                                ColumnType {
                                    scalar_type: String,
                                    nullable: false,
                                },
                                ColumnType {
                                    scalar_type: Int64,
                                    nullable: false,
                                },
                                ColumnType {
                                    scalar_type: String,
                                    nullable: false,
                                },
                                ColumnType {
                                    scalar_type: Numeric {
                                        max_scale: Some(
                                            NumericMaxScale(
                                                2,
                                            ),
                                        ),
                                    },
                                    nullable: false,
                                },
                                ColumnType {
                                    scalar_type: String,
                                    nullable: false,
                                },
                                ColumnType {
                                    scalar_type: String,
                                    nullable: false,
                                },
                            ],
                            keys: [
                                [
                                    0,
                                ],
                            ],
                        },
                    },
                    keys: [
                        [
                            Column(
                                2,
                            ),
                        ],
                    ],
                },
            ),
        },
    ],
    index_exports: {
        User(
            16,
        ): (
            IndexDesc {
                on_id: User(
                    5,
                ),
                key: [
                    Column(
                        2,
                    ),
                ],
            },
            RelationType {
                column_types: [
                    ColumnType {
                        scalar_type: Int64,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: String,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: String,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: Int64,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: String,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: Numeric {
                            max_scale: Some(
                                NumericMaxScale(
                                    2,
                                ),
                            ),
                        },
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: String,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: String,
                        nullable: false,
                    },
                ],
                keys: [
                    [
                        0,
                    ],
                ],
            },
        ),
    },
    sink_exports: {},
    as_of: None,
    until: Antichain {
        elements: [],
    },
    debug_name: "materialize.public.customer_address",
}
```