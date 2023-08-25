```text
DataflowDescription {
    source_imports: {
        User(
            1,
        ): (
            SourceInstanceDesc {
                arguments: SourceInstanceArguments {
                    operators: Some(
                        MapFilterProject {
                            expressions: [],
                            predicates: [
                                (
                                    3,
                                    CallBinary {
                                        func: Eq,
                                        expr1: Column(
                                            2,
                                        ),
                                        expr2: Literal(
                                            Ok(
                                                Row{[
                                                    Int64(
                                                        100,
                                                    ),
                                                ]},
                                            ),
                                            ColumnType {
                                                scalar_type: Int64,
                                                nullable: false,
                                            },
                                        ),
                                    },
                                ),
                            ],
                            projection: [
                                0,
                                1,
                                2,
                            ],
                            input_arity: 3,
                        },
                    ),
                },
                storage_metadata: (),
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
            false,
        ),
    },
    index_imports: {},
    objects_to_build: [
        BuildDesc {
            id: Transient(
                105,
            ),
            plan: OptimizedMirRelationExpr(
                Filter {
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
                    predicates: [
                        CallBinary {
                            func: Eq,
                            expr1: Column(
                                2,
                            ),
                            expr2: Literal(
                                Ok(
                                    Row{[
                                        Int64(
                                            100,
                                        ),
                                    ]},
                                ),
                                ColumnType {
                                    scalar_type: Int64,
                                    nullable: false,
                                },
                            ),
                        },
                    ],
                },
            ),
        },
    ],
    index_exports: {},
    sink_exports: {
        User(
            8,
        ): ComputeSinkDesc {
            from: Transient(
                105,
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