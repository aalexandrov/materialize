```text
DataflowDescription {
    source_imports: {},
    index_imports: {
        User(
            9,
        ): IndexImport {
            desc: IndexDesc {
                on_id: User(
                    1,
                ),
                key: [
                    Column(
                        2,
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
                        scalar_type: Int64,
                        nullable: false,
                    },
                    ColumnType {
                        scalar_type: Int64,
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
                    Lookup,
                ],
            ),
        },
    },
    objects_to_build: [
        BuildDesc {
            id: Transient(
                1652,
            ),
            plan: OptimizedMirRelationExpr(
                Project {
                    input: Join {
                        inputs: [
                            ArrangeBy {
                                input: Get {
                                    id: Global(
                                        User(
                                            1,
                                        ),
                                    ),
                                    typ: RelationType {
                                        column_types: [
                                            ColumnType {
                                                scalar_type: Int64,
                                                nullable: false,
                                            },
                                            ColumnType {
                                                scalar_type: Int64,
                                                nullable: false,
                                            },
                                            ColumnType {
                                                scalar_type: Int64,
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
                            ArrangeBy {
                                input: Constant {
                                    rows: Ok(
                                        [
                                            (
                                                Row{[
                                                    Int64(
                                                        100,
                                                    ),
                                                ]},
                                                1,
                                            ),
                                        ],
                                    ),
                                    typ: RelationType {
                                        column_types: [
                                            ColumnType {
                                                scalar_type: Int64,
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
                                            0,
                                        ),
                                    ],
                                ],
                            },
                        ],
                        equivalences: [
                            [
                                Column(
                                    2,
                                ),
                                Column(
                                    3,
                                ),
                            ],
                        ],
                        implementation: IndexedFilter(
                            User(
                                1,
                            ),
                            [
                                Column(
                                    2,
                                ),
                            ],
                            [
                                Row{[
                                    Int64(
                                        100,
                                    ),
                                ]},
                            ],
                        ),
                    },
                    outputs: [
                        0,
                        1,
                        2,
                    ],
                },
            ),
        },
    ],
    index_exports: {},
    sink_exports: {
        User(
            10,
        ): ComputeSinkDesc {
            from: Transient(
                1652,
            ),
            from_desc: RelationDesc {
                typ: RelationType {
                    column_types: [
                        ColumnType {
                            scalar_type: Int64,
                            nullable: false,
                        },
                        ColumnType {
                            scalar_type: Int64,
                            nullable: false,
                        },
                        ColumnType {
                            scalar_type: Int64,
                            nullable: false,
                        },
                    ],
                    keys: [
                        [
                            0,
                        ],
                    ],
                },
                names: [
                    ColumnName(
                        "id",
                    ),
                    ColumnName(
                        "org_id",
                    ),
                    ColumnName(
                        "balance",
                    ),
                ],
            },
            connection: Persist(
                PersistSinkConnection {
                    value_desc: RelationDesc {
                        typ: RelationType {
                            column_types: [
                                ColumnType {
                                    scalar_type: Int64,
                                    nullable: false,
                                },
                                ColumnType {
                                    scalar_type: Int64,
                                    nullable: false,
                                },
                                ColumnType {
                                    scalar_type: Int64,
                                    nullable: false,
                                },
                            ],
                            keys: [
                                [
                                    0,
                                ],
                            ],
                        },
                        names: [
                            ColumnName(
                                "id",
                            ),
                            ColumnName(
                                "org_id",
                            ),
                            ColumnName(
                                "balance",
                            ),
                        ],
                    },
                    storage_metadata: (),
                },
            ),
            with_snapshot: true,
            up_to: Antichain {
                elements: [],
            },
        },
    },
    as_of: None,
    until: Antichain {
        elements: [],
    },
    debug_name: "materialize.public.mv",
}
```