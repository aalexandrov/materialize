```
DataflowDescription {
    source_imports: {},
    index_imports: {
        User(
            3,
        ): IndexImport {
            desc: IndexDesc {
                on_id: User(
                    2,
                ),
                key: [
                    Column(
                        0,
                    ),
                ],
            },
            typ: RelationType {
                column_types: [
                    ColumnType {
                        scalar_type: Int32,
                        nullable: true,
                    },
                    ColumnType {
                        scalar_type: Int32,
                        nullable: true,
                    },
                    ColumnType {
                        scalar_type: Int32,
                        nullable: true,
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
                4,
            ),
            plan: OptimizedMirRelationExpr(
                ArrangeBy {
                    input: Get {
                        id: Global(
                            User(
                                2,
                            ),
                        ),
                        typ: RelationType {
                            column_types: [
                                ColumnType {
                                    scalar_type: Int32,
                                    nullable: true,
                                },
                                ColumnType {
                                    scalar_type: Int32,
                                    nullable: true,
                                },
                                ColumnType {
                                    scalar_type: Int32,
                                    nullable: true,
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
            4,
        ): (
            IndexDesc {
                on_id: User(
                    2,
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
                        scalar_type: Int32,
                        nullable: true,
                    },
                    ColumnType {
                        scalar_type: Int32,
                        nullable: true,
                    },
                    ColumnType {
                        scalar_type: Int32,
                        nullable: true,
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
    debug_name: "materialize.public.v_idx2",
}
```