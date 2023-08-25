```
DataflowDescription {
    source_imports: {
        User(
            1,
        ): (
            SourceInstanceDesc {
                arguments: SourceInstanceArguments {
                    operators: None,
                },
                storage_metadata: (),
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
                    keys: [],
                },
            },
            false,
        ),
    },
    index_imports: {},
    objects_to_build: [
        BuildDesc {
            id: User(
                2,
            ),
            plan: OptimizedMirRelationExpr(
                TopK {
                    input: TopK {
                        input: Get {
                            id: Global(
                                User(
                                    1,
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
                                keys: [],
                            },
                        },
                        group_key: [
                            0,
                        ],
                        order_key: [],
                        limit: Some(
                            1,
                        ),
                        offset: 0,
                        monotonic: false,
                        expected_group_size: None,
                    },
                    group_key: [],
                    order_key: [],
                    limit: Some(
                        10,
                    ),
                    offset: 0,
                    monotonic: false,
                    expected_group_size: None,
                },
            ),
        },
        BuildDesc {
            id: User(
                3,
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
                                0,
                            ),
                        ],
                    ],
                },
            ),
        },
    ],
    index_exports: {
        User(
            3,
        ): (
            IndexDesc {
                on_id: User(
                    2,
                ),
                key: [
                    Column(
                        0,
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
    debug_name: "materialize.public.v_idx",
}
```