```
DataflowDescription {
    source_imports: {
        User(
            5,
        ): (
            SourceInstanceDesc {
                arguments: SourceInstanceArguments {
                    operators: None,
                },
                storage_metadata: (),
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
            false,
        ),
    },
    index_imports: {},
    objects_to_build: [
        BuildDesc {
            id: User(
                15,
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
                                1,
                            ),
                        ],
                    ],
                },
            ),
        },
    ],
    index_exports: {
        User(
            15,
        ): (
            IndexDesc {
                on_id: User(
                    5,
                ),
                key: [
                    Column(
                        1,
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
    debug_name: "materialize.public.customer_name",
}
```