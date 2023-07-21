# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Source definitions
# ------------------

# Define x source
define
DefSource name=x keys=[[#0]]
  - bigint?
----
Source defined as t0

# Define y source
define
DefSource name=y keys=[[#0, #1]]
  - bigint?
  - bigint?
----
Source defined as t1

# ProvInfo inference (basic cases)
# --------------------------------

# WMR handling
apply pipeline=redundant_join
Return
  Get l1
With Mutually Recursive
  cte l1 = // { types: "(bigint)" }
    Return
      Threshold
        Union
          Get l1
          Negate
            Get l0
    With Mutually Recursive
      cte l0 = // { types: "(bigint)" }
        Union
          Get l1
          Get l0
----
Return
  Get l1
With Mutually Recursive
  cte l1 =
    Return
      Threshold
        Union
          Get l1
          Negate
            Get l0
    With Mutually Recursive
      cte l0 =
        Union
          Get l1
          Get l0

# Distinct handling.
#
# This is sensitive to skipping empty projections the `is_trivial` method
# when pruning `ProvInfo` instances.
apply pipeline=redundant_join
Project (#1, #2, #0)
  Map (123, 0)
    Join on=(0 = #0)
      Distinct
        Get x
      Filter (#0) IS NOT NULL
        Get x
----
Project (#1, #2, #0)
  Map (123, 0)
    Project (#0)
      Join on=(#0 = 0)
        Filter (#0) IS NOT NULL
          Get x

# Map + Project With Let/Get bindings.
# [
#   ProvInfo {
#       id: t0,
#       dereferenced_projection: [#0, _],
#       exact: false,
#   },
#   ProvInfo {
#       id: l0,
#       dereferenced_projection: [#0, _],
#       exact: false,
#   },
#   ProvInfo {
#       id: t0,
#       dereferenced_projection: [_, _],
#       exact: false,
#   },
# ]
apply pipeline=redundant_join
Return
  Project (#0)
    Filter #200
      Join on=(#0 = #199)
        Get l296
        Union
          Get l298
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l298
                Distinct group_by=[#0]
                  Get l297
              Get l297
              Constant // { types: "(boolean)" }
                - (false)
With
  cte l298 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 100) AND (#0 = #1)
          CrossJoin
            Get l297
            Get x
  cte l297 =
    Distinct group_by=[#0]
      Get l296
  cte l296 =
    Filter #198
      Join on=(#0 = #197)
        Get l293
        Union
          Get l295
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l295
                Distinct group_by=[#0]
                  Get l294
              Get l294
              Constant // { types: "(boolean)" }
                - (false)
  cte l295 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 99) AND (#0 = #1)
          CrossJoin
            Get l294
            Get x
  cte l294 =
    Distinct group_by=[#0]
      Get l293
  cte l293 =
    Filter #196
      Join on=(#0 = #195)
        Get l290
        Union
          Get l292
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l292
                Distinct group_by=[#0]
                  Get l291
              Get l291
              Constant // { types: "(boolean)" }
                - (false)
  cte l292 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 98) AND (#0 = #1)
          CrossJoin
            Get l291
            Get x
  cte l291 =
    Distinct group_by=[#0]
      Get l290
  cte l290 =
    Filter #194
      Join on=(#0 = #193)
        Get l287
        Union
          Get l289
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l289
                Distinct group_by=[#0]
                  Get l288
              Get l288
              Constant // { types: "(boolean)" }
                - (false)
  cte l289 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 97) AND (#0 = #1)
          CrossJoin
            Get l288
            Get x
  cte l288 =
    Distinct group_by=[#0]
      Get l287
  cte l287 =
    Filter #192
      Join on=(#0 = #191)
        Get l284
        Union
          Get l286
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l286
                Distinct group_by=[#0]
                  Get l285
              Get l285
              Constant // { types: "(boolean)" }
                - (false)
  cte l286 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 96) AND (#0 = #1)
          CrossJoin
            Get l285
            Get x
  cte l285 =
    Distinct group_by=[#0]
      Get l284
  cte l284 =
    Filter #190
      Join on=(#0 = #189)
        Get l281
        Union
          Get l283
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l283
                Distinct group_by=[#0]
                  Get l282
              Get l282
              Constant // { types: "(boolean)" }
                - (false)
  cte l283 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 95) AND (#0 = #1)
          CrossJoin
            Get l282
            Get x
  cte l282 =
    Distinct group_by=[#0]
      Get l281
  cte l281 =
    Filter #188
      Join on=(#0 = #187)
        Get l278
        Union
          Get l280
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l280
                Distinct group_by=[#0]
                  Get l279
              Get l279
              Constant // { types: "(boolean)" }
                - (false)
  cte l280 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 94) AND (#0 = #1)
          CrossJoin
            Get l279
            Get x
  cte l279 =
    Distinct group_by=[#0]
      Get l278
  cte l278 =
    Filter #186
      Join on=(#0 = #185)
        Get l275
        Union
          Get l277
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l277
                Distinct group_by=[#0]
                  Get l276
              Get l276
              Constant // { types: "(boolean)" }
                - (false)
  cte l277 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 93) AND (#0 = #1)
          CrossJoin
            Get l276
            Get x
  cte l276 =
    Distinct group_by=[#0]
      Get l275
  cte l275 =
    Filter #184
      Join on=(#0 = #183)
        Get l272
        Union
          Get l274
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l274
                Distinct group_by=[#0]
                  Get l273
              Get l273
              Constant // { types: "(boolean)" }
                - (false)
  cte l274 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 92) AND (#0 = #1)
          CrossJoin
            Get l273
            Get x
  cte l273 =
    Distinct group_by=[#0]
      Get l272
  cte l272 =
    Filter #182
      Join on=(#0 = #181)
        Get l269
        Union
          Get l271
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l271
                Distinct group_by=[#0]
                  Get l270
              Get l270
              Constant // { types: "(boolean)" }
                - (false)
  cte l271 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 91) AND (#0 = #1)
          CrossJoin
            Get l270
            Get x
  cte l270 =
    Distinct group_by=[#0]
      Get l269
  cte l269 =
    Filter #180
      Join on=(#0 = #179)
        Get l266
        Union
          Get l268
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l268
                Distinct group_by=[#0]
                  Get l267
              Get l267
              Constant // { types: "(boolean)" }
                - (false)
  cte l268 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 90) AND (#0 = #1)
          CrossJoin
            Get l267
            Get x
  cte l267 =
    Distinct group_by=[#0]
      Get l266
  cte l266 =
    Filter #178
      Join on=(#0 = #177)
        Get l263
        Union
          Get l265
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l265
                Distinct group_by=[#0]
                  Get l264
              Get l264
              Constant // { types: "(boolean)" }
                - (false)
  cte l265 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 89) AND (#0 = #1)
          CrossJoin
            Get l264
            Get x
  cte l264 =
    Distinct group_by=[#0]
      Get l263
  cte l263 =
    Filter #176
      Join on=(#0 = #175)
        Get l260
        Union
          Get l262
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l262
                Distinct group_by=[#0]
                  Get l261
              Get l261
              Constant // { types: "(boolean)" }
                - (false)
  cte l262 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 88) AND (#0 = #1)
          CrossJoin
            Get l261
            Get x
  cte l261 =
    Distinct group_by=[#0]
      Get l260
  cte l260 =
    Filter #174
      Join on=(#0 = #173)
        Get l257
        Union
          Get l259
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l259
                Distinct group_by=[#0]
                  Get l258
              Get l258
              Constant // { types: "(boolean)" }
                - (false)
  cte l259 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 87) AND (#0 = #1)
          CrossJoin
            Get l258
            Get x
  cte l258 =
    Distinct group_by=[#0]
      Get l257
  cte l257 =
    Filter #172
      Join on=(#0 = #171)
        Get l254
        Union
          Get l256
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l256
                Distinct group_by=[#0]
                  Get l255
              Get l255
              Constant // { types: "(boolean)" }
                - (false)
  cte l256 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 86) AND (#0 = #1)
          CrossJoin
            Get l255
            Get x
  cte l255 =
    Distinct group_by=[#0]
      Get l254
  cte l254 =
    Filter #170
      Join on=(#0 = #169)
        Get l251
        Union
          Get l253
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l253
                Distinct group_by=[#0]
                  Get l252
              Get l252
              Constant // { types: "(boolean)" }
                - (false)
  cte l253 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 85) AND (#0 = #1)
          CrossJoin
            Get l252
            Get x
  cte l252 =
    Distinct group_by=[#0]
      Get l251
  cte l251 =
    Filter #168
      Join on=(#0 = #167)
        Get l248
        Union
          Get l250
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l250
                Distinct group_by=[#0]
                  Get l249
              Get l249
              Constant // { types: "(boolean)" }
                - (false)
  cte l250 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 84) AND (#0 = #1)
          CrossJoin
            Get l249
            Get x
  cte l249 =
    Distinct group_by=[#0]
      Get l248
  cte l248 =
    Filter #166
      Join on=(#0 = #165)
        Get l245
        Union
          Get l247
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l247
                Distinct group_by=[#0]
                  Get l246
              Get l246
              Constant // { types: "(boolean)" }
                - (false)
  cte l247 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 83) AND (#0 = #1)
          CrossJoin
            Get l246
            Get x
  cte l246 =
    Distinct group_by=[#0]
      Get l245
  cte l245 =
    Filter #164
      Join on=(#0 = #163)
        Get l242
        Union
          Get l244
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l244
                Distinct group_by=[#0]
                  Get l243
              Get l243
              Constant // { types: "(boolean)" }
                - (false)
  cte l244 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 82) AND (#0 = #1)
          CrossJoin
            Get l243
            Get x
  cte l243 =
    Distinct group_by=[#0]
      Get l242
  cte l242 =
    Filter #162
      Join on=(#0 = #161)
        Get l239
        Union
          Get l241
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l241
                Distinct group_by=[#0]
                  Get l240
              Get l240
              Constant // { types: "(boolean)" }
                - (false)
  cte l241 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 81) AND (#0 = #1)
          CrossJoin
            Get l240
            Get x
  cte l240 =
    Distinct group_by=[#0]
      Get l239
  cte l239 =
    Filter #160
      Join on=(#0 = #159)
        Get l236
        Union
          Get l238
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l238
                Distinct group_by=[#0]
                  Get l237
              Get l237
              Constant // { types: "(boolean)" }
                - (false)
  cte l238 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 80) AND (#0 = #1)
          CrossJoin
            Get l237
            Get x
  cte l237 =
    Distinct group_by=[#0]
      Get l236
  cte l236 =
    Filter #158
      Join on=(#0 = #157)
        Get l233
        Union
          Get l235
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l235
                Distinct group_by=[#0]
                  Get l234
              Get l234
              Constant // { types: "(boolean)" }
                - (false)
  cte l235 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 79) AND (#0 = #1)
          CrossJoin
            Get l234
            Get x
  cte l234 =
    Distinct group_by=[#0]
      Get l233
  cte l233 =
    Filter #156
      Join on=(#0 = #155)
        Get l230
        Union
          Get l232
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l232
                Distinct group_by=[#0]
                  Get l231
              Get l231
              Constant // { types: "(boolean)" }
                - (false)
  cte l232 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 78) AND (#0 = #1)
          CrossJoin
            Get l231
            Get x
  cte l231 =
    Distinct group_by=[#0]
      Get l230
  cte l230 =
    Filter #154
      Join on=(#0 = #153)
        Get l227
        Union
          Get l229
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l229
                Distinct group_by=[#0]
                  Get l228
              Get l228
              Constant // { types: "(boolean)" }
                - (false)
  cte l229 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 77) AND (#0 = #1)
          CrossJoin
            Get l228
            Get x
  cte l228 =
    Distinct group_by=[#0]
      Get l227
  cte l227 =
    Filter #152
      Join on=(#0 = #151)
        Get l224
        Union
          Get l226
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l226
                Distinct group_by=[#0]
                  Get l225
              Get l225
              Constant // { types: "(boolean)" }
                - (false)
  cte l226 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 76) AND (#0 = #1)
          CrossJoin
            Get l225
            Get x
  cte l225 =
    Distinct group_by=[#0]
      Get l224
  cte l224 =
    Filter #150
      Join on=(#0 = #149)
        Get l221
        Union
          Get l223
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l223
                Distinct group_by=[#0]
                  Get l222
              Get l222
              Constant // { types: "(boolean)" }
                - (false)
  cte l223 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 75) AND (#0 = #1)
          CrossJoin
            Get l222
            Get x
  cte l222 =
    Distinct group_by=[#0]
      Get l221
  cte l221 =
    Filter #148
      Join on=(#0 = #147)
        Get l218
        Union
          Get l220
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l220
                Distinct group_by=[#0]
                  Get l219
              Get l219
              Constant // { types: "(boolean)" }
                - (false)
  cte l220 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 74) AND (#0 = #1)
          CrossJoin
            Get l219
            Get x
  cte l219 =
    Distinct group_by=[#0]
      Get l218
  cte l218 =
    Filter #146
      Join on=(#0 = #145)
        Get l215
        Union
          Get l217
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l217
                Distinct group_by=[#0]
                  Get l216
              Get l216
              Constant // { types: "(boolean)" }
                - (false)
  cte l217 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 73) AND (#0 = #1)
          CrossJoin
            Get l216
            Get x
  cte l216 =
    Distinct group_by=[#0]
      Get l215
  cte l215 =
    Filter #144
      Join on=(#0 = #143)
        Get l212
        Union
          Get l214
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l214
                Distinct group_by=[#0]
                  Get l213
              Get l213
              Constant // { types: "(boolean)" }
                - (false)
  cte l214 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 72) AND (#0 = #1)
          CrossJoin
            Get l213
            Get x
  cte l213 =
    Distinct group_by=[#0]
      Get l212
  cte l212 =
    Filter #142
      Join on=(#0 = #141)
        Get l209
        Union
          Get l211
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l211
                Distinct group_by=[#0]
                  Get l210
              Get l210
              Constant // { types: "(boolean)" }
                - (false)
  cte l211 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 71) AND (#0 = #1)
          CrossJoin
            Get l210
            Get x
  cte l210 =
    Distinct group_by=[#0]
      Get l209
  cte l209 =
    Filter #140
      Join on=(#0 = #139)
        Get l206
        Union
          Get l208
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l208
                Distinct group_by=[#0]
                  Get l207
              Get l207
              Constant // { types: "(boolean)" }
                - (false)
  cte l208 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 70) AND (#0 = #1)
          CrossJoin
            Get l207
            Get x
  cte l207 =
    Distinct group_by=[#0]
      Get l206
  cte l206 =
    Filter #138
      Join on=(#0 = #137)
        Get l203
        Union
          Get l205
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l205
                Distinct group_by=[#0]
                  Get l204
              Get l204
              Constant // { types: "(boolean)" }
                - (false)
  cte l205 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 69) AND (#0 = #1)
          CrossJoin
            Get l204
            Get x
  cte l204 =
    Distinct group_by=[#0]
      Get l203
  cte l203 =
    Filter #136
      Join on=(#0 = #135)
        Get l200
        Union
          Get l202
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l202
                Distinct group_by=[#0]
                  Get l201
              Get l201
              Constant // { types: "(boolean)" }
                - (false)
  cte l202 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 68) AND (#0 = #1)
          CrossJoin
            Get l201
            Get x
  cte l201 =
    Distinct group_by=[#0]
      Get l200
  cte l200 =
    Filter #134
      Join on=(#0 = #133)
        Get l197
        Union
          Get l199
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l199
                Distinct group_by=[#0]
                  Get l198
              Get l198
              Constant // { types: "(boolean)" }
                - (false)
  cte l199 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 67) AND (#0 = #1)
          CrossJoin
            Get l198
            Get x
  cte l198 =
    Distinct group_by=[#0]
      Get l197
  cte l197 =
    Filter #132
      Join on=(#0 = #131)
        Get l194
        Union
          Get l196
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l196
                Distinct group_by=[#0]
                  Get l195
              Get l195
              Constant // { types: "(boolean)" }
                - (false)
  cte l196 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 66) AND (#0 = #1)
          CrossJoin
            Get l195
            Get x
  cte l195 =
    Distinct group_by=[#0]
      Get l194
  cte l194 =
    Filter #130
      Join on=(#0 = #129)
        Get l191
        Union
          Get l193
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l193
                Distinct group_by=[#0]
                  Get l192
              Get l192
              Constant // { types: "(boolean)" }
                - (false)
  cte l193 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 65) AND (#0 = #1)
          CrossJoin
            Get l192
            Get x
  cte l192 =
    Distinct group_by=[#0]
      Get l191
  cte l191 =
    Filter #128
      Join on=(#0 = #127)
        Get l188
        Union
          Get l190
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l190
                Distinct group_by=[#0]
                  Get l189
              Get l189
              Constant // { types: "(boolean)" }
                - (false)
  cte l190 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 64) AND (#0 = #1)
          CrossJoin
            Get l189
            Get x
  cte l189 =
    Distinct group_by=[#0]
      Get l188
  cte l188 =
    Filter #126
      Join on=(#0 = #125)
        Get l185
        Union
          Get l187
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l187
                Distinct group_by=[#0]
                  Get l186
              Get l186
              Constant // { types: "(boolean)" }
                - (false)
  cte l187 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 63) AND (#0 = #1)
          CrossJoin
            Get l186
            Get x
  cte l186 =
    Distinct group_by=[#0]
      Get l185
  cte l185 =
    Filter #124
      Join on=(#0 = #123)
        Get l182
        Union
          Get l184
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l184
                Distinct group_by=[#0]
                  Get l183
              Get l183
              Constant // { types: "(boolean)" }
                - (false)
  cte l184 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 62) AND (#0 = #1)
          CrossJoin
            Get l183
            Get x
  cte l183 =
    Distinct group_by=[#0]
      Get l182
  cte l182 =
    Filter #122
      Join on=(#0 = #121)
        Get l179
        Union
          Get l181
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l181
                Distinct group_by=[#0]
                  Get l180
              Get l180
              Constant // { types: "(boolean)" }
                - (false)
  cte l181 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 61) AND (#0 = #1)
          CrossJoin
            Get l180
            Get x
  cte l180 =
    Distinct group_by=[#0]
      Get l179
  cte l179 =
    Filter #120
      Join on=(#0 = #119)
        Get l176
        Union
          Get l178
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l178
                Distinct group_by=[#0]
                  Get l177
              Get l177
              Constant // { types: "(boolean)" }
                - (false)
  cte l178 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 60) AND (#0 = #1)
          CrossJoin
            Get l177
            Get x
  cte l177 =
    Distinct group_by=[#0]
      Get l176
  cte l176 =
    Filter #118
      Join on=(#0 = #117)
        Get l173
        Union
          Get l175
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l175
                Distinct group_by=[#0]
                  Get l174
              Get l174
              Constant // { types: "(boolean)" }
                - (false)
  cte l175 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 59) AND (#0 = #1)
          CrossJoin
            Get l174
            Get x
  cte l174 =
    Distinct group_by=[#0]
      Get l173
  cte l173 =
    Filter #116
      Join on=(#0 = #115)
        Get l170
        Union
          Get l172
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l172
                Distinct group_by=[#0]
                  Get l171
              Get l171
              Constant // { types: "(boolean)" }
                - (false)
  cte l172 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 58) AND (#0 = #1)
          CrossJoin
            Get l171
            Get x
  cte l171 =
    Distinct group_by=[#0]
      Get l170
  cte l170 =
    Filter #114
      Join on=(#0 = #113)
        Get l167
        Union
          Get l169
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l169
                Distinct group_by=[#0]
                  Get l168
              Get l168
              Constant // { types: "(boolean)" }
                - (false)
  cte l169 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 57) AND (#0 = #1)
          CrossJoin
            Get l168
            Get x
  cte l168 =
    Distinct group_by=[#0]
      Get l167
  cte l167 =
    Filter #112
      Join on=(#0 = #111)
        Get l164
        Union
          Get l166
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l166
                Distinct group_by=[#0]
                  Get l165
              Get l165
              Constant // { types: "(boolean)" }
                - (false)
  cte l166 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 56) AND (#0 = #1)
          CrossJoin
            Get l165
            Get x
  cte l165 =
    Distinct group_by=[#0]
      Get l164
  cte l164 =
    Filter #110
      Join on=(#0 = #109)
        Get l161
        Union
          Get l163
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l163
                Distinct group_by=[#0]
                  Get l162
              Get l162
              Constant // { types: "(boolean)" }
                - (false)
  cte l163 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 55) AND (#0 = #1)
          CrossJoin
            Get l162
            Get x
  cte l162 =
    Distinct group_by=[#0]
      Get l161
  cte l161 =
    Filter #108
      Join on=(#0 = #107)
        Get l158
        Union
          Get l160
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l160
                Distinct group_by=[#0]
                  Get l159
              Get l159
              Constant // { types: "(boolean)" }
                - (false)
  cte l160 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 54) AND (#0 = #1)
          CrossJoin
            Get l159
            Get x
  cte l159 =
    Distinct group_by=[#0]
      Get l158
  cte l158 =
    Filter #106
      Join on=(#0 = #105)
        Get l155
        Union
          Get l157
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l157
                Distinct group_by=[#0]
                  Get l156
              Get l156
              Constant // { types: "(boolean)" }
                - (false)
  cte l157 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 53) AND (#0 = #1)
          CrossJoin
            Get l156
            Get x
  cte l156 =
    Distinct group_by=[#0]
      Get l155
  cte l155 =
    Filter #104
      Join on=(#0 = #103)
        Get l152
        Union
          Get l154
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l154
                Distinct group_by=[#0]
                  Get l153
              Get l153
              Constant // { types: "(boolean)" }
                - (false)
  cte l154 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 52) AND (#0 = #1)
          CrossJoin
            Get l153
            Get x
  cte l153 =
    Distinct group_by=[#0]
      Get l152
  cte l152 =
    Filter #102
      Join on=(#0 = #101)
        Get l149
        Union
          Get l151
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l151
                Distinct group_by=[#0]
                  Get l150
              Get l150
              Constant // { types: "(boolean)" }
                - (false)
  cte l151 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 51) AND (#0 = #1)
          CrossJoin
            Get l150
            Get x
  cte l150 =
    Distinct group_by=[#0]
      Get l149
  cte l149 =
    Filter #100
      Join on=(#0 = #99)
        Get l146
        Union
          Get l148
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l148
                Distinct group_by=[#0]
                  Get l147
              Get l147
              Constant // { types: "(boolean)" }
                - (false)
  cte l148 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 50) AND (#0 = #1)
          CrossJoin
            Get l147
            Get x
  cte l147 =
    Distinct group_by=[#0]
      Get l146
  cte l146 =
    Filter #98
      Join on=(#0 = #97)
        Get l143
        Union
          Get l145
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l145
                Distinct group_by=[#0]
                  Get l144
              Get l144
              Constant // { types: "(boolean)" }
                - (false)
  cte l145 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 49) AND (#0 = #1)
          CrossJoin
            Get l144
            Get x
  cte l144 =
    Distinct group_by=[#0]
      Get l143
  cte l143 =
    Filter #96
      Join on=(#0 = #95)
        Get l140
        Union
          Get l142
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l142
                Distinct group_by=[#0]
                  Get l141
              Get l141
              Constant // { types: "(boolean)" }
                - (false)
  cte l142 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 48) AND (#0 = #1)
          CrossJoin
            Get l141
            Get x
  cte l141 =
    Distinct group_by=[#0]
      Get l140
  cte l140 =
    Filter #94
      Join on=(#0 = #93)
        Get l137
        Union
          Get l139
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l139
                Distinct group_by=[#0]
                  Get l138
              Get l138
              Constant // { types: "(boolean)" }
                - (false)
  cte l139 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 47) AND (#0 = #1)
          CrossJoin
            Get l138
            Get x
  cte l138 =
    Distinct group_by=[#0]
      Get l137
  cte l137 =
    Filter #92
      Join on=(#0 = #91)
        Get l134
        Union
          Get l136
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l136
                Distinct group_by=[#0]
                  Get l135
              Get l135
              Constant // { types: "(boolean)" }
                - (false)
  cte l136 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 46) AND (#0 = #1)
          CrossJoin
            Get l135
            Get x
  cte l135 =
    Distinct group_by=[#0]
      Get l134
  cte l134 =
    Filter #90
      Join on=(#0 = #89)
        Get l131
        Union
          Get l133
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l133
                Distinct group_by=[#0]
                  Get l132
              Get l132
              Constant // { types: "(boolean)" }
                - (false)
  cte l133 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 45) AND (#0 = #1)
          CrossJoin
            Get l132
            Get x
  cte l132 =
    Distinct group_by=[#0]
      Get l131
  cte l131 =
    Filter #88
      Join on=(#0 = #87)
        Get l128
        Union
          Get l130
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l130
                Distinct group_by=[#0]
                  Get l129
              Get l129
              Constant // { types: "(boolean)" }
                - (false)
  cte l130 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 44) AND (#0 = #1)
          CrossJoin
            Get l129
            Get x
  cte l129 =
    Distinct group_by=[#0]
      Get l128
  cte l128 =
    Filter #86
      Join on=(#0 = #85)
        Get l125
        Union
          Get l127
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l127
                Distinct group_by=[#0]
                  Get l126
              Get l126
              Constant // { types: "(boolean)" }
                - (false)
  cte l127 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 43) AND (#0 = #1)
          CrossJoin
            Get l126
            Get x
  cte l126 =
    Distinct group_by=[#0]
      Get l125
  cte l125 =
    Filter #84
      Join on=(#0 = #83)
        Get l122
        Union
          Get l124
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l124
                Distinct group_by=[#0]
                  Get l123
              Get l123
              Constant // { types: "(boolean)" }
                - (false)
  cte l124 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 42) AND (#0 = #1)
          CrossJoin
            Get l123
            Get x
  cte l123 =
    Distinct group_by=[#0]
      Get l122
  cte l122 =
    Filter #82
      Join on=(#0 = #81)
        Get l119
        Union
          Get l121
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l121
                Distinct group_by=[#0]
                  Get l120
              Get l120
              Constant // { types: "(boolean)" }
                - (false)
  cte l121 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 41) AND (#0 = #1)
          CrossJoin
            Get l120
            Get x
  cte l120 =
    Distinct group_by=[#0]
      Get l119
  cte l119 =
    Filter #80
      Join on=(#0 = #79)
        Get l116
        Union
          Get l118
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l118
                Distinct group_by=[#0]
                  Get l117
              Get l117
              Constant // { types: "(boolean)" }
                - (false)
  cte l118 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 40) AND (#0 = #1)
          CrossJoin
            Get l117
            Get x
  cte l117 =
    Distinct group_by=[#0]
      Get l116
  cte l116 =
    Filter #78
      Join on=(#0 = #77)
        Get l113
        Union
          Get l115
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l115
                Distinct group_by=[#0]
                  Get l114
              Get l114
              Constant // { types: "(boolean)" }
                - (false)
  cte l115 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 39) AND (#0 = #1)
          CrossJoin
            Get l114
            Get x
  cte l114 =
    Distinct group_by=[#0]
      Get l113
  cte l113 =
    Filter #76
      Join on=(#0 = #75)
        Get l110
        Union
          Get l112
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l112
                Distinct group_by=[#0]
                  Get l111
              Get l111
              Constant // { types: "(boolean)" }
                - (false)
  cte l112 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 38) AND (#0 = #1)
          CrossJoin
            Get l111
            Get x
  cte l111 =
    Distinct group_by=[#0]
      Get l110
  cte l110 =
    Filter #74
      Join on=(#0 = #73)
        Get l107
        Union
          Get l109
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l109
                Distinct group_by=[#0]
                  Get l108
              Get l108
              Constant // { types: "(boolean)" }
                - (false)
  cte l109 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 37) AND (#0 = #1)
          CrossJoin
            Get l108
            Get x
  cte l108 =
    Distinct group_by=[#0]
      Get l107
  cte l107 =
    Filter #72
      Join on=(#0 = #71)
        Get l104
        Union
          Get l106
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l106
                Distinct group_by=[#0]
                  Get l105
              Get l105
              Constant // { types: "(boolean)" }
                - (false)
  cte l106 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 36) AND (#0 = #1)
          CrossJoin
            Get l105
            Get x
  cte l105 =
    Distinct group_by=[#0]
      Get l104
  cte l104 =
    Filter #70
      Join on=(#0 = #69)
        Get l101
        Union
          Get l103
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l103
                Distinct group_by=[#0]
                  Get l102
              Get l102
              Constant // { types: "(boolean)" }
                - (false)
  cte l103 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 35) AND (#0 = #1)
          CrossJoin
            Get l102
            Get x
  cte l102 =
    Distinct group_by=[#0]
      Get l101
  cte l101 =
    Filter #68
      Join on=(#0 = #67)
        Get l98
        Union
          Get l100
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l100
                Distinct group_by=[#0]
                  Get l99
              Get l99
              Constant // { types: "(boolean)" }
                - (false)
  cte l100 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 34) AND (#0 = #1)
          CrossJoin
            Get l99
            Get x
  cte l99 =
    Distinct group_by=[#0]
      Get l98
  cte l98 =
    Filter #66
      Join on=(#0 = #65)
        Get l95
        Union
          Get l97
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l97
                Distinct group_by=[#0]
                  Get l96
              Get l96
              Constant // { types: "(boolean)" }
                - (false)
  cte l97 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 33) AND (#0 = #1)
          CrossJoin
            Get l96
            Get x
  cte l96 =
    Distinct group_by=[#0]
      Get l95
  cte l95 =
    Filter #64
      Join on=(#0 = #63)
        Get l92
        Union
          Get l94
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l94
                Distinct group_by=[#0]
                  Get l93
              Get l93
              Constant // { types: "(boolean)" }
                - (false)
  cte l94 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 32) AND (#0 = #1)
          CrossJoin
            Get l93
            Get x
  cte l93 =
    Distinct group_by=[#0]
      Get l92
  cte l92 =
    Filter #62
      Join on=(#0 = #61)
        Get l89
        Union
          Get l91
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l91
                Distinct group_by=[#0]
                  Get l90
              Get l90
              Constant // { types: "(boolean)" }
                - (false)
  cte l91 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 31) AND (#0 = #1)
          CrossJoin
            Get l90
            Get x
  cte l90 =
    Distinct group_by=[#0]
      Get l89
  cte l89 =
    Filter #60
      Join on=(#0 = #59)
        Get l86
        Union
          Get l88
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l88
                Distinct group_by=[#0]
                  Get l87
              Get l87
              Constant // { types: "(boolean)" }
                - (false)
  cte l88 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 30) AND (#0 = #1)
          CrossJoin
            Get l87
            Get x
  cte l87 =
    Distinct group_by=[#0]
      Get l86
  cte l86 =
    Filter #58
      Join on=(#0 = #57)
        Get l83
        Union
          Get l85
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l85
                Distinct group_by=[#0]
                  Get l84
              Get l84
              Constant // { types: "(boolean)" }
                - (false)
  cte l85 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 29) AND (#0 = #1)
          CrossJoin
            Get l84
            Get x
  cte l84 =
    Distinct group_by=[#0]
      Get l83
  cte l83 =
    Filter #56
      Join on=(#0 = #55)
        Get l80
        Union
          Get l82
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l82
                Distinct group_by=[#0]
                  Get l81
              Get l81
              Constant // { types: "(boolean)" }
                - (false)
  cte l82 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 28) AND (#0 = #1)
          CrossJoin
            Get l81
            Get x
  cte l81 =
    Distinct group_by=[#0]
      Get l80
  cte l80 =
    Filter #54
      Join on=(#0 = #53)
        Get l77
        Union
          Get l79
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l79
                Distinct group_by=[#0]
                  Get l78
              Get l78
              Constant // { types: "(boolean)" }
                - (false)
  cte l79 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 27) AND (#0 = #1)
          CrossJoin
            Get l78
            Get x
  cte l78 =
    Distinct group_by=[#0]
      Get l77
  cte l77 =
    Filter #52
      Join on=(#0 = #51)
        Get l74
        Union
          Get l76
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l76
                Distinct group_by=[#0]
                  Get l75
              Get l75
              Constant // { types: "(boolean)" }
                - (false)
  cte l76 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 26) AND (#0 = #1)
          CrossJoin
            Get l75
            Get x
  cte l75 =
    Distinct group_by=[#0]
      Get l74
  cte l74 =
    Filter #50
      Join on=(#0 = #49)
        Get l71
        Union
          Get l73
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l73
                Distinct group_by=[#0]
                  Get l72
              Get l72
              Constant // { types: "(boolean)" }
                - (false)
  cte l73 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 25) AND (#0 = #1)
          CrossJoin
            Get l72
            Get x
  cte l72 =
    Distinct group_by=[#0]
      Get l71
  cte l71 =
    Filter #48
      Join on=(#0 = #47)
        Get l68
        Union
          Get l70
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l70
                Distinct group_by=[#0]
                  Get l69
              Get l69
              Constant // { types: "(boolean)" }
                - (false)
  cte l70 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 24) AND (#0 = #1)
          CrossJoin
            Get l69
            Get x
  cte l69 =
    Distinct group_by=[#0]
      Get l68
  cte l68 =
    Filter #46
      Join on=(#0 = #45)
        Get l65
        Union
          Get l67
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l67
                Distinct group_by=[#0]
                  Get l66
              Get l66
              Constant // { types: "(boolean)" }
                - (false)
  cte l67 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 23) AND (#0 = #1)
          CrossJoin
            Get l66
            Get x
  cte l66 =
    Distinct group_by=[#0]
      Get l65
  cte l65 =
    Filter #44
      Join on=(#0 = #43)
        Get l62
        Union
          Get l64
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l64
                Distinct group_by=[#0]
                  Get l63
              Get l63
              Constant // { types: "(boolean)" }
                - (false)
  cte l64 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 22) AND (#0 = #1)
          CrossJoin
            Get l63
            Get x
  cte l63 =
    Distinct group_by=[#0]
      Get l62
  cte l62 =
    Filter #42
      Join on=(#0 = #41)
        Get l59
        Union
          Get l61
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l61
                Distinct group_by=[#0]
                  Get l60
              Get l60
              Constant // { types: "(boolean)" }
                - (false)
  cte l61 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 21) AND (#0 = #1)
          CrossJoin
            Get l60
            Get x
  cte l60 =
    Distinct group_by=[#0]
      Get l59
  cte l59 =
    Filter #40
      Join on=(#0 = #39)
        Get l56
        Union
          Get l58
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l58
                Distinct group_by=[#0]
                  Get l57
              Get l57
              Constant // { types: "(boolean)" }
                - (false)
  cte l58 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 20) AND (#0 = #1)
          CrossJoin
            Get l57
            Get x
  cte l57 =
    Distinct group_by=[#0]
      Get l56
  cte l56 =
    Filter #38
      Join on=(#0 = #37)
        Get l53
        Union
          Get l55
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l55
                Distinct group_by=[#0]
                  Get l54
              Get l54
              Constant // { types: "(boolean)" }
                - (false)
  cte l55 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 19) AND (#0 = #1)
          CrossJoin
            Get l54
            Get x
  cte l54 =
    Distinct group_by=[#0]
      Get l53
  cte l53 =
    Filter #36
      Join on=(#0 = #35)
        Get l50
        Union
          Get l52
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l52
                Distinct group_by=[#0]
                  Get l51
              Get l51
              Constant // { types: "(boolean)" }
                - (false)
  cte l52 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 18) AND (#0 = #1)
          CrossJoin
            Get l51
            Get x
  cte l51 =
    Distinct group_by=[#0]
      Get l50
  cte l50 =
    Filter #34
      Join on=(#0 = #33)
        Get l47
        Union
          Get l49
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l49
                Distinct group_by=[#0]
                  Get l48
              Get l48
              Constant // { types: "(boolean)" }
                - (false)
  cte l49 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 17) AND (#0 = #1)
          CrossJoin
            Get l48
            Get x
  cte l48 =
    Distinct group_by=[#0]
      Get l47
  cte l47 =
    Filter #32
      Join on=(#0 = #31)
        Get l44
        Union
          Get l46
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l46
                Distinct group_by=[#0]
                  Get l45
              Get l45
              Constant // { types: "(boolean)" }
                - (false)
  cte l46 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 16) AND (#0 = #1)
          CrossJoin
            Get l45
            Get x
  cte l45 =
    Distinct group_by=[#0]
      Get l44
  cte l44 =
    Filter #30
      Join on=(#0 = #29)
        Get l41
        Union
          Get l43
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l43
                Distinct group_by=[#0]
                  Get l42
              Get l42
              Constant // { types: "(boolean)" }
                - (false)
  cte l43 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 15) AND (#0 = #1)
          CrossJoin
            Get l42
            Get x
  cte l42 =
    Distinct group_by=[#0]
      Get l41
  cte l41 =
    Filter #28
      Join on=(#0 = #27)
        Get l38
        Union
          Get l40
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l40
                Distinct group_by=[#0]
                  Get l39
              Get l39
              Constant // { types: "(boolean)" }
                - (false)
  cte l40 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 14) AND (#0 = #1)
          CrossJoin
            Get l39
            Get x
  cte l39 =
    Distinct group_by=[#0]
      Get l38
  cte l38 =
    Filter #26
      Join on=(#0 = #25)
        Get l35
        Union
          Get l37
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l37
                Distinct group_by=[#0]
                  Get l36
              Get l36
              Constant // { types: "(boolean)" }
                - (false)
  cte l37 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 13) AND (#0 = #1)
          CrossJoin
            Get l36
            Get x
  cte l36 =
    Distinct group_by=[#0]
      Get l35
  cte l35 =
    Filter #24
      Join on=(#0 = #23)
        Get l32
        Union
          Get l34
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l34
                Distinct group_by=[#0]
                  Get l33
              Get l33
              Constant // { types: "(boolean)" }
                - (false)
  cte l34 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 12) AND (#0 = #1)
          CrossJoin
            Get l33
            Get x
  cte l33 =
    Distinct group_by=[#0]
      Get l32
  cte l32 =
    Filter #22
      Join on=(#0 = #21)
        Get l29
        Union
          Get l31
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l31
                Distinct group_by=[#0]
                  Get l30
              Get l30
              Constant // { types: "(boolean)" }
                - (false)
  cte l31 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 11) AND (#0 = #1)
          CrossJoin
            Get l30
            Get x
  cte l30 =
    Distinct group_by=[#0]
      Get l29
  cte l29 =
    Filter #20
      Join on=(#0 = #19)
        Get l26
        Union
          Get l28
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l28
                Distinct group_by=[#0]
                  Get l27
              Get l27
              Constant // { types: "(boolean)" }
                - (false)
  cte l28 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 10) AND (#0 = #1)
          CrossJoin
            Get l27
            Get x
  cte l27 =
    Distinct group_by=[#0]
      Get l26
  cte l26 =
    Filter #18
      Join on=(#0 = #17)
        Get l23
        Union
          Get l25
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l25
                Distinct group_by=[#0]
                  Get l24
              Get l24
              Constant // { types: "(boolean)" }
                - (false)
  cte l25 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 9) AND (#0 = #1)
          CrossJoin
            Get l24
            Get x
  cte l24 =
    Distinct group_by=[#0]
      Get l23
  cte l23 =
    Filter #16
      Join on=(#0 = #15)
        Get l20
        Union
          Get l22
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l22
                Distinct group_by=[#0]
                  Get l21
              Get l21
              Constant // { types: "(boolean)" }
                - (false)
  cte l22 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 8) AND (#0 = #1)
          CrossJoin
            Get l21
            Get x
  cte l21 =
    Distinct group_by=[#0]
      Get l20
  cte l20 =
    Filter #14
      Join on=(#0 = #13)
        Get l17
        Union
          Get l19
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l19
                Distinct group_by=[#0]
                  Get l18
              Get l18
              Constant // { types: "(boolean)" }
                - (false)
  cte l19 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 7) AND (#0 = #1)
          CrossJoin
            Get l18
            Get x
  cte l18 =
    Distinct group_by=[#0]
      Get l17
  cte l17 =
    Filter #12
      Join on=(#0 = #11)
        Get l14
        Union
          Get l16
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l16
                Distinct group_by=[#0]
                  Get l15
              Get l15
              Constant // { types: "(boolean)" }
                - (false)
  cte l16 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 6) AND (#0 = #1)
          CrossJoin
            Get l15
            Get x
  cte l15 =
    Distinct group_by=[#0]
      Get l14
  cte l14 =
    Filter #10
      Join on=(#0 = #9)
        Get l11
        Union
          Get l13
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l13
                Distinct group_by=[#0]
                  Get l12
              Get l12
              Constant // { types: "(boolean)" }
                - (false)
  cte l13 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 5) AND (#0 = #1)
          CrossJoin
            Get l12
            Get x
  cte l12 =
    Distinct group_by=[#0]
      Get l11
  cte l11 =
    Filter #8
      Join on=(#0 = #7)
        Get l8
        Union
          Get l10
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l10
                Distinct group_by=[#0]
                  Get l9
              Get l9
              Constant // { types: "(boolean)" }
                - (false)
  cte l10 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 4) AND (#0 = #1)
          CrossJoin
            Get l9
            Get x
  cte l9 =
    Distinct group_by=[#0]
      Get l8
  cte l8 =
    Filter #6
      Join on=(#0 = #5)
        Get l5
        Union
          Get l7
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l7
                Distinct group_by=[#0]
                  Get l6
              Get l6
              Constant // { types: "(boolean)" }
                - (false)
  cte l7 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 3) AND (#0 = #1)
          CrossJoin
            Get l6
            Get x
  cte l6 =
    Distinct group_by=[#0]
      Get l5
  cte l5 =
    Filter #4
      Join on=(#0 = #3)
        Get l2
        Union
          Get l4
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l4
                Distinct group_by=[#0]
                  Get l3
              Get l3
              Constant // { types: "(boolean)" }
                - (false)
  cte l4 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 2) AND (#0 = #1)
          CrossJoin
            Get l3
            Get x
  cte l3 =
    Distinct group_by=[#0]
      Get l2
  cte l2 =
    Filter #2
      Join on=(#0 = #1)
        Get x
        Union
          Get l1
          Project (#0, #2)
            Join on=(#0 = #1)
              Union
                Negate
                  Distinct group_by=[#0]
                    Get l1
                Distinct group_by=[#0]
                  Get l0
              Get l0
              Constant // { types: "(boolean)" }
                - (false)
  cte l1 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 1) AND (#0 = #1)
          CrossJoin
            Get l0
            Get x
  cte l0 =
    Distinct group_by=[#0]
      Get x
----
Return
  Project (#0)
    Filter #200
      Join on=(#0 = #199)
        Get l296
        Union
          Get l298
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l298
                    Distinct group_by=[#0]
                      Get l297
                  Constant
                    - (false)
With
  cte l298 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 100) AND (#0 = #1)
          CrossJoin
            Get l297
            Get x
  cte l297 =
    Distinct group_by=[#0]
      Get l296
  cte l296 =
    Filter #198
      Join on=(#0 = #197)
        Get l293
        Union
          Get l295
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l295
                    Distinct group_by=[#0]
                      Get l294
                  Constant
                    - (false)
  cte l295 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 99) AND (#0 = #1)
          CrossJoin
            Get l294
            Get x
  cte l294 =
    Distinct group_by=[#0]
      Get l293
  cte l293 =
    Filter #196
      Join on=(#0 = #195)
        Get l290
        Union
          Get l292
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l292
                    Distinct group_by=[#0]
                      Get l291
                  Constant
                    - (false)
  cte l292 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 98) AND (#0 = #1)
          CrossJoin
            Get l291
            Get x
  cte l291 =
    Distinct group_by=[#0]
      Get l290
  cte l290 =
    Filter #194
      Join on=(#0 = #193)
        Get l287
        Union
          Get l289
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l289
                    Distinct group_by=[#0]
                      Get l288
                  Constant
                    - (false)
  cte l289 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 97) AND (#0 = #1)
          CrossJoin
            Get l288
            Get x
  cte l288 =
    Distinct group_by=[#0]
      Get l287
  cte l287 =
    Filter #192
      Join on=(#0 = #191)
        Get l284
        Union
          Get l286
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l286
                    Distinct group_by=[#0]
                      Get l285
                  Constant
                    - (false)
  cte l286 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 96) AND (#0 = #1)
          CrossJoin
            Get l285
            Get x
  cte l285 =
    Distinct group_by=[#0]
      Get l284
  cte l284 =
    Filter #190
      Join on=(#0 = #189)
        Get l281
        Union
          Get l283
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l283
                    Distinct group_by=[#0]
                      Get l282
                  Constant
                    - (false)
  cte l283 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 95) AND (#0 = #1)
          CrossJoin
            Get l282
            Get x
  cte l282 =
    Distinct group_by=[#0]
      Get l281
  cte l281 =
    Filter #188
      Join on=(#0 = #187)
        Get l278
        Union
          Get l280
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l280
                    Distinct group_by=[#0]
                      Get l279
                  Constant
                    - (false)
  cte l280 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 94) AND (#0 = #1)
          CrossJoin
            Get l279
            Get x
  cte l279 =
    Distinct group_by=[#0]
      Get l278
  cte l278 =
    Filter #186
      Join on=(#0 = #185)
        Get l275
        Union
          Get l277
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l277
                    Distinct group_by=[#0]
                      Get l276
                  Constant
                    - (false)
  cte l277 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 93) AND (#0 = #1)
          CrossJoin
            Get l276
            Get x
  cte l276 =
    Distinct group_by=[#0]
      Get l275
  cte l275 =
    Filter #184
      Join on=(#0 = #183)
        Get l272
        Union
          Get l274
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l274
                    Distinct group_by=[#0]
                      Get l273
                  Constant
                    - (false)
  cte l274 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 92) AND (#0 = #1)
          CrossJoin
            Get l273
            Get x
  cte l273 =
    Distinct group_by=[#0]
      Get l272
  cte l272 =
    Filter #182
      Join on=(#0 = #181)
        Get l269
        Union
          Get l271
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l271
                    Distinct group_by=[#0]
                      Get l270
                  Constant
                    - (false)
  cte l271 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 91) AND (#0 = #1)
          CrossJoin
            Get l270
            Get x
  cte l270 =
    Distinct group_by=[#0]
      Get l269
  cte l269 =
    Filter #180
      Join on=(#0 = #179)
        Get l266
        Union
          Get l268
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l268
                    Distinct group_by=[#0]
                      Get l267
                  Constant
                    - (false)
  cte l268 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 90) AND (#0 = #1)
          CrossJoin
            Get l267
            Get x
  cte l267 =
    Distinct group_by=[#0]
      Get l266
  cte l266 =
    Filter #178
      Join on=(#0 = #177)
        Get l263
        Union
          Get l265
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l265
                    Distinct group_by=[#0]
                      Get l264
                  Constant
                    - (false)
  cte l265 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 89) AND (#0 = #1)
          CrossJoin
            Get l264
            Get x
  cte l264 =
    Distinct group_by=[#0]
      Get l263
  cte l263 =
    Filter #176
      Join on=(#0 = #175)
        Get l260
        Union
          Get l262
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l262
                    Distinct group_by=[#0]
                      Get l261
                  Constant
                    - (false)
  cte l262 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 88) AND (#0 = #1)
          CrossJoin
            Get l261
            Get x
  cte l261 =
    Distinct group_by=[#0]
      Get l260
  cte l260 =
    Filter #174
      Join on=(#0 = #173)
        Get l257
        Union
          Get l259
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l259
                    Distinct group_by=[#0]
                      Get l258
                  Constant
                    - (false)
  cte l259 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 87) AND (#0 = #1)
          CrossJoin
            Get l258
            Get x
  cte l258 =
    Distinct group_by=[#0]
      Get l257
  cte l257 =
    Filter #172
      Join on=(#0 = #171)
        Get l254
        Union
          Get l256
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l256
                    Distinct group_by=[#0]
                      Get l255
                  Constant
                    - (false)
  cte l256 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 86) AND (#0 = #1)
          CrossJoin
            Get l255
            Get x
  cte l255 =
    Distinct group_by=[#0]
      Get l254
  cte l254 =
    Filter #170
      Join on=(#0 = #169)
        Get l251
        Union
          Get l253
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l253
                    Distinct group_by=[#0]
                      Get l252
                  Constant
                    - (false)
  cte l253 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 85) AND (#0 = #1)
          CrossJoin
            Get l252
            Get x
  cte l252 =
    Distinct group_by=[#0]
      Get l251
  cte l251 =
    Filter #168
      Join on=(#0 = #167)
        Get l248
        Union
          Get l250
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l250
                    Distinct group_by=[#0]
                      Get l249
                  Constant
                    - (false)
  cte l250 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 84) AND (#0 = #1)
          CrossJoin
            Get l249
            Get x
  cte l249 =
    Distinct group_by=[#0]
      Get l248
  cte l248 =
    Filter #166
      Join on=(#0 = #165)
        Get l245
        Union
          Get l247
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l247
                    Distinct group_by=[#0]
                      Get l246
                  Constant
                    - (false)
  cte l247 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 83) AND (#0 = #1)
          CrossJoin
            Get l246
            Get x
  cte l246 =
    Distinct group_by=[#0]
      Get l245
  cte l245 =
    Filter #164
      Join on=(#0 = #163)
        Get l242
        Union
          Get l244
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l244
                    Distinct group_by=[#0]
                      Get l243
                  Constant
                    - (false)
  cte l244 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 82) AND (#0 = #1)
          CrossJoin
            Get l243
            Get x
  cte l243 =
    Distinct group_by=[#0]
      Get l242
  cte l242 =
    Filter #162
      Join on=(#0 = #161)
        Get l239
        Union
          Get l241
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l241
                    Distinct group_by=[#0]
                      Get l240
                  Constant
                    - (false)
  cte l241 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 81) AND (#0 = #1)
          CrossJoin
            Get l240
            Get x
  cte l240 =
    Distinct group_by=[#0]
      Get l239
  cte l239 =
    Filter #160
      Join on=(#0 = #159)
        Get l236
        Union
          Get l238
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l238
                    Distinct group_by=[#0]
                      Get l237
                  Constant
                    - (false)
  cte l238 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 80) AND (#0 = #1)
          CrossJoin
            Get l237
            Get x
  cte l237 =
    Distinct group_by=[#0]
      Get l236
  cte l236 =
    Filter #158
      Join on=(#0 = #157)
        Get l233
        Union
          Get l235
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l235
                    Distinct group_by=[#0]
                      Get l234
                  Constant
                    - (false)
  cte l235 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 79) AND (#0 = #1)
          CrossJoin
            Get l234
            Get x
  cte l234 =
    Distinct group_by=[#0]
      Get l233
  cte l233 =
    Filter #156
      Join on=(#0 = #155)
        Get l230
        Union
          Get l232
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l232
                    Distinct group_by=[#0]
                      Get l231
                  Constant
                    - (false)
  cte l232 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 78) AND (#0 = #1)
          CrossJoin
            Get l231
            Get x
  cte l231 =
    Distinct group_by=[#0]
      Get l230
  cte l230 =
    Filter #154
      Join on=(#0 = #153)
        Get l227
        Union
          Get l229
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l229
                    Distinct group_by=[#0]
                      Get l228
                  Constant
                    - (false)
  cte l229 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 77) AND (#0 = #1)
          CrossJoin
            Get l228
            Get x
  cte l228 =
    Distinct group_by=[#0]
      Get l227
  cte l227 =
    Filter #152
      Join on=(#0 = #151)
        Get l224
        Union
          Get l226
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l226
                    Distinct group_by=[#0]
                      Get l225
                  Constant
                    - (false)
  cte l226 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 76) AND (#0 = #1)
          CrossJoin
            Get l225
            Get x
  cte l225 =
    Distinct group_by=[#0]
      Get l224
  cte l224 =
    Filter #150
      Join on=(#0 = #149)
        Get l221
        Union
          Get l223
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l223
                    Distinct group_by=[#0]
                      Get l222
                  Constant
                    - (false)
  cte l223 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 75) AND (#0 = #1)
          CrossJoin
            Get l222
            Get x
  cte l222 =
    Distinct group_by=[#0]
      Get l221
  cte l221 =
    Filter #148
      Join on=(#0 = #147)
        Get l218
        Union
          Get l220
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l220
                    Distinct group_by=[#0]
                      Get l219
                  Constant
                    - (false)
  cte l220 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 74) AND (#0 = #1)
          CrossJoin
            Get l219
            Get x
  cte l219 =
    Distinct group_by=[#0]
      Get l218
  cte l218 =
    Filter #146
      Join on=(#0 = #145)
        Get l215
        Union
          Get l217
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l217
                    Distinct group_by=[#0]
                      Get l216
                  Constant
                    - (false)
  cte l217 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 73) AND (#0 = #1)
          CrossJoin
            Get l216
            Get x
  cte l216 =
    Distinct group_by=[#0]
      Get l215
  cte l215 =
    Filter #144
      Join on=(#0 = #143)
        Get l212
        Union
          Get l214
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l214
                    Distinct group_by=[#0]
                      Get l213
                  Constant
                    - (false)
  cte l214 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 72) AND (#0 = #1)
          CrossJoin
            Get l213
            Get x
  cte l213 =
    Distinct group_by=[#0]
      Get l212
  cte l212 =
    Filter #142
      Join on=(#0 = #141)
        Get l209
        Union
          Get l211
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l211
                    Distinct group_by=[#0]
                      Get l210
                  Constant
                    - (false)
  cte l211 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 71) AND (#0 = #1)
          CrossJoin
            Get l210
            Get x
  cte l210 =
    Distinct group_by=[#0]
      Get l209
  cte l209 =
    Filter #140
      Join on=(#0 = #139)
        Get l206
        Union
          Get l208
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l208
                    Distinct group_by=[#0]
                      Get l207
                  Constant
                    - (false)
  cte l208 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 70) AND (#0 = #1)
          CrossJoin
            Get l207
            Get x
  cte l207 =
    Distinct group_by=[#0]
      Get l206
  cte l206 =
    Filter #138
      Join on=(#0 = #137)
        Get l203
        Union
          Get l205
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l205
                    Distinct group_by=[#0]
                      Get l204
                  Constant
                    - (false)
  cte l205 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 69) AND (#0 = #1)
          CrossJoin
            Get l204
            Get x
  cte l204 =
    Distinct group_by=[#0]
      Get l203
  cte l203 =
    Filter #136
      Join on=(#0 = #135)
        Get l200
        Union
          Get l202
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l202
                    Distinct group_by=[#0]
                      Get l201
                  Constant
                    - (false)
  cte l202 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 68) AND (#0 = #1)
          CrossJoin
            Get l201
            Get x
  cte l201 =
    Distinct group_by=[#0]
      Get l200
  cte l200 =
    Filter #134
      Join on=(#0 = #133)
        Get l197
        Union
          Get l199
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l199
                    Distinct group_by=[#0]
                      Get l198
                  Constant
                    - (false)
  cte l199 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 67) AND (#0 = #1)
          CrossJoin
            Get l198
            Get x
  cte l198 =
    Distinct group_by=[#0]
      Get l197
  cte l197 =
    Filter #132
      Join on=(#0 = #131)
        Get l194
        Union
          Get l196
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l196
                    Distinct group_by=[#0]
                      Get l195
                  Constant
                    - (false)
  cte l196 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 66) AND (#0 = #1)
          CrossJoin
            Get l195
            Get x
  cte l195 =
    Distinct group_by=[#0]
      Get l194
  cte l194 =
    Filter #130
      Join on=(#0 = #129)
        Get l191
        Union
          Get l193
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l193
                    Distinct group_by=[#0]
                      Get l192
                  Constant
                    - (false)
  cte l193 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 65) AND (#0 = #1)
          CrossJoin
            Get l192
            Get x
  cte l192 =
    Distinct group_by=[#0]
      Get l191
  cte l191 =
    Filter #128
      Join on=(#0 = #127)
        Get l188
        Union
          Get l190
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l190
                    Distinct group_by=[#0]
                      Get l189
                  Constant
                    - (false)
  cte l190 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 64) AND (#0 = #1)
          CrossJoin
            Get l189
            Get x
  cte l189 =
    Distinct group_by=[#0]
      Get l188
  cte l188 =
    Filter #126
      Join on=(#0 = #125)
        Get l185
        Union
          Get l187
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l187
                    Distinct group_by=[#0]
                      Get l186
                  Constant
                    - (false)
  cte l187 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 63) AND (#0 = #1)
          CrossJoin
            Get l186
            Get x
  cte l186 =
    Distinct group_by=[#0]
      Get l185
  cte l185 =
    Filter #124
      Join on=(#0 = #123)
        Get l182
        Union
          Get l184
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l184
                    Distinct group_by=[#0]
                      Get l183
                  Constant
                    - (false)
  cte l184 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 62) AND (#0 = #1)
          CrossJoin
            Get l183
            Get x
  cte l183 =
    Distinct group_by=[#0]
      Get l182
  cte l182 =
    Filter #122
      Join on=(#0 = #121)
        Get l179
        Union
          Get l181
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l181
                    Distinct group_by=[#0]
                      Get l180
                  Constant
                    - (false)
  cte l181 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 61) AND (#0 = #1)
          CrossJoin
            Get l180
            Get x
  cte l180 =
    Distinct group_by=[#0]
      Get l179
  cte l179 =
    Filter #120
      Join on=(#0 = #119)
        Get l176
        Union
          Get l178
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l178
                    Distinct group_by=[#0]
                      Get l177
                  Constant
                    - (false)
  cte l178 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 60) AND (#0 = #1)
          CrossJoin
            Get l177
            Get x
  cte l177 =
    Distinct group_by=[#0]
      Get l176
  cte l176 =
    Filter #118
      Join on=(#0 = #117)
        Get l173
        Union
          Get l175
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l175
                    Distinct group_by=[#0]
                      Get l174
                  Constant
                    - (false)
  cte l175 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 59) AND (#0 = #1)
          CrossJoin
            Get l174
            Get x
  cte l174 =
    Distinct group_by=[#0]
      Get l173
  cte l173 =
    Filter #116
      Join on=(#0 = #115)
        Get l170
        Union
          Get l172
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l172
                    Distinct group_by=[#0]
                      Get l171
                  Constant
                    - (false)
  cte l172 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 58) AND (#0 = #1)
          CrossJoin
            Get l171
            Get x
  cte l171 =
    Distinct group_by=[#0]
      Get l170
  cte l170 =
    Filter #114
      Join on=(#0 = #113)
        Get l167
        Union
          Get l169
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l169
                    Distinct group_by=[#0]
                      Get l168
                  Constant
                    - (false)
  cte l169 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 57) AND (#0 = #1)
          CrossJoin
            Get l168
            Get x
  cte l168 =
    Distinct group_by=[#0]
      Get l167
  cte l167 =
    Filter #112
      Join on=(#0 = #111)
        Get l164
        Union
          Get l166
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l166
                    Distinct group_by=[#0]
                      Get l165
                  Constant
                    - (false)
  cte l166 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 56) AND (#0 = #1)
          CrossJoin
            Get l165
            Get x
  cte l165 =
    Distinct group_by=[#0]
      Get l164
  cte l164 =
    Filter #110
      Join on=(#0 = #109)
        Get l161
        Union
          Get l163
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l163
                    Distinct group_by=[#0]
                      Get l162
                  Constant
                    - (false)
  cte l163 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 55) AND (#0 = #1)
          CrossJoin
            Get l162
            Get x
  cte l162 =
    Distinct group_by=[#0]
      Get l161
  cte l161 =
    Filter #108
      Join on=(#0 = #107)
        Get l158
        Union
          Get l160
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l160
                    Distinct group_by=[#0]
                      Get l159
                  Constant
                    - (false)
  cte l160 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 54) AND (#0 = #1)
          CrossJoin
            Get l159
            Get x
  cte l159 =
    Distinct group_by=[#0]
      Get l158
  cte l158 =
    Filter #106
      Join on=(#0 = #105)
        Get l155
        Union
          Get l157
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l157
                    Distinct group_by=[#0]
                      Get l156
                  Constant
                    - (false)
  cte l157 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 53) AND (#0 = #1)
          CrossJoin
            Get l156
            Get x
  cte l156 =
    Distinct group_by=[#0]
      Get l155
  cte l155 =
    Filter #104
      Join on=(#0 = #103)
        Get l152
        Union
          Get l154
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l154
                    Distinct group_by=[#0]
                      Get l153
                  Constant
                    - (false)
  cte l154 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 52) AND (#0 = #1)
          CrossJoin
            Get l153
            Get x
  cte l153 =
    Distinct group_by=[#0]
      Get l152
  cte l152 =
    Filter #102
      Join on=(#0 = #101)
        Get l149
        Union
          Get l151
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l151
                    Distinct group_by=[#0]
                      Get l150
                  Constant
                    - (false)
  cte l151 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 51) AND (#0 = #1)
          CrossJoin
            Get l150
            Get x
  cte l150 =
    Distinct group_by=[#0]
      Get l149
  cte l149 =
    Filter #100
      Join on=(#0 = #99)
        Get l146
        Union
          Get l148
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l148
                    Distinct group_by=[#0]
                      Get l147
                  Constant
                    - (false)
  cte l148 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 50) AND (#0 = #1)
          CrossJoin
            Get l147
            Get x
  cte l147 =
    Distinct group_by=[#0]
      Get l146
  cte l146 =
    Filter #98
      Join on=(#0 = #97)
        Get l143
        Union
          Get l145
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l145
                    Distinct group_by=[#0]
                      Get l144
                  Constant
                    - (false)
  cte l145 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 49) AND (#0 = #1)
          CrossJoin
            Get l144
            Get x
  cte l144 =
    Distinct group_by=[#0]
      Get l143
  cte l143 =
    Filter #96
      Join on=(#0 = #95)
        Get l140
        Union
          Get l142
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l142
                    Distinct group_by=[#0]
                      Get l141
                  Constant
                    - (false)
  cte l142 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 48) AND (#0 = #1)
          CrossJoin
            Get l141
            Get x
  cte l141 =
    Distinct group_by=[#0]
      Get l140
  cte l140 =
    Filter #94
      Join on=(#0 = #93)
        Get l137
        Union
          Get l139
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l139
                    Distinct group_by=[#0]
                      Get l138
                  Constant
                    - (false)
  cte l139 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 47) AND (#0 = #1)
          CrossJoin
            Get l138
            Get x
  cte l138 =
    Distinct group_by=[#0]
      Get l137
  cte l137 =
    Filter #92
      Join on=(#0 = #91)
        Get l134
        Union
          Get l136
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l136
                    Distinct group_by=[#0]
                      Get l135
                  Constant
                    - (false)
  cte l136 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 46) AND (#0 = #1)
          CrossJoin
            Get l135
            Get x
  cte l135 =
    Distinct group_by=[#0]
      Get l134
  cte l134 =
    Filter #90
      Join on=(#0 = #89)
        Get l131
        Union
          Get l133
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l133
                    Distinct group_by=[#0]
                      Get l132
                  Constant
                    - (false)
  cte l133 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 45) AND (#0 = #1)
          CrossJoin
            Get l132
            Get x
  cte l132 =
    Distinct group_by=[#0]
      Get l131
  cte l131 =
    Filter #88
      Join on=(#0 = #87)
        Get l128
        Union
          Get l130
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l130
                    Distinct group_by=[#0]
                      Get l129
                  Constant
                    - (false)
  cte l130 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 44) AND (#0 = #1)
          CrossJoin
            Get l129
            Get x
  cte l129 =
    Distinct group_by=[#0]
      Get l128
  cte l128 =
    Filter #86
      Join on=(#0 = #85)
        Get l125
        Union
          Get l127
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l127
                    Distinct group_by=[#0]
                      Get l126
                  Constant
                    - (false)
  cte l127 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 43) AND (#0 = #1)
          CrossJoin
            Get l126
            Get x
  cte l126 =
    Distinct group_by=[#0]
      Get l125
  cte l125 =
    Filter #84
      Join on=(#0 = #83)
        Get l122
        Union
          Get l124
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l124
                    Distinct group_by=[#0]
                      Get l123
                  Constant
                    - (false)
  cte l124 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 42) AND (#0 = #1)
          CrossJoin
            Get l123
            Get x
  cte l123 =
    Distinct group_by=[#0]
      Get l122
  cte l122 =
    Filter #82
      Join on=(#0 = #81)
        Get l119
        Union
          Get l121
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l121
                    Distinct group_by=[#0]
                      Get l120
                  Constant
                    - (false)
  cte l121 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 41) AND (#0 = #1)
          CrossJoin
            Get l120
            Get x
  cte l120 =
    Distinct group_by=[#0]
      Get l119
  cte l119 =
    Filter #80
      Join on=(#0 = #79)
        Get l116
        Union
          Get l118
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l118
                    Distinct group_by=[#0]
                      Get l117
                  Constant
                    - (false)
  cte l118 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 40) AND (#0 = #1)
          CrossJoin
            Get l117
            Get x
  cte l117 =
    Distinct group_by=[#0]
      Get l116
  cte l116 =
    Filter #78
      Join on=(#0 = #77)
        Get l113
        Union
          Get l115
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l115
                    Distinct group_by=[#0]
                      Get l114
                  Constant
                    - (false)
  cte l115 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 39) AND (#0 = #1)
          CrossJoin
            Get l114
            Get x
  cte l114 =
    Distinct group_by=[#0]
      Get l113
  cte l113 =
    Filter #76
      Join on=(#0 = #75)
        Get l110
        Union
          Get l112
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l112
                    Distinct group_by=[#0]
                      Get l111
                  Constant
                    - (false)
  cte l112 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 38) AND (#0 = #1)
          CrossJoin
            Get l111
            Get x
  cte l111 =
    Distinct group_by=[#0]
      Get l110
  cte l110 =
    Filter #74
      Join on=(#0 = #73)
        Get l107
        Union
          Get l109
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l109
                    Distinct group_by=[#0]
                      Get l108
                  Constant
                    - (false)
  cte l109 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 37) AND (#0 = #1)
          CrossJoin
            Get l108
            Get x
  cte l108 =
    Distinct group_by=[#0]
      Get l107
  cte l107 =
    Filter #72
      Join on=(#0 = #71)
        Get l104
        Union
          Get l106
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l106
                    Distinct group_by=[#0]
                      Get l105
                  Constant
                    - (false)
  cte l106 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 36) AND (#0 = #1)
          CrossJoin
            Get l105
            Get x
  cte l105 =
    Distinct group_by=[#0]
      Get l104
  cte l104 =
    Filter #70
      Join on=(#0 = #69)
        Get l101
        Union
          Get l103
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l103
                    Distinct group_by=[#0]
                      Get l102
                  Constant
                    - (false)
  cte l103 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 35) AND (#0 = #1)
          CrossJoin
            Get l102
            Get x
  cte l102 =
    Distinct group_by=[#0]
      Get l101
  cte l101 =
    Filter #68
      Join on=(#0 = #67)
        Get l98
        Union
          Get l100
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l100
                    Distinct group_by=[#0]
                      Get l99
                  Constant
                    - (false)
  cte l100 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 34) AND (#0 = #1)
          CrossJoin
            Get l99
            Get x
  cte l99 =
    Distinct group_by=[#0]
      Get l98
  cte l98 =
    Filter #66
      Join on=(#0 = #65)
        Get l95
        Union
          Get l97
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l97
                    Distinct group_by=[#0]
                      Get l96
                  Constant
                    - (false)
  cte l97 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 33) AND (#0 = #1)
          CrossJoin
            Get l96
            Get x
  cte l96 =
    Distinct group_by=[#0]
      Get l95
  cte l95 =
    Filter #64
      Join on=(#0 = #63)
        Get l92
        Union
          Get l94
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l94
                    Distinct group_by=[#0]
                      Get l93
                  Constant
                    - (false)
  cte l94 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 32) AND (#0 = #1)
          CrossJoin
            Get l93
            Get x
  cte l93 =
    Distinct group_by=[#0]
      Get l92
  cte l92 =
    Filter #62
      Join on=(#0 = #61)
        Get l89
        Union
          Get l91
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l91
                    Distinct group_by=[#0]
                      Get l90
                  Constant
                    - (false)
  cte l91 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 31) AND (#0 = #1)
          CrossJoin
            Get l90
            Get x
  cte l90 =
    Distinct group_by=[#0]
      Get l89
  cte l89 =
    Filter #60
      Join on=(#0 = #59)
        Get l86
        Union
          Get l88
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l88
                    Distinct group_by=[#0]
                      Get l87
                  Constant
                    - (false)
  cte l88 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 30) AND (#0 = #1)
          CrossJoin
            Get l87
            Get x
  cte l87 =
    Distinct group_by=[#0]
      Get l86
  cte l86 =
    Filter #58
      Join on=(#0 = #57)
        Get l83
        Union
          Get l85
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l85
                    Distinct group_by=[#0]
                      Get l84
                  Constant
                    - (false)
  cte l85 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 29) AND (#0 = #1)
          CrossJoin
            Get l84
            Get x
  cte l84 =
    Distinct group_by=[#0]
      Get l83
  cte l83 =
    Filter #56
      Join on=(#0 = #55)
        Get l80
        Union
          Get l82
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l82
                    Distinct group_by=[#0]
                      Get l81
                  Constant
                    - (false)
  cte l82 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 28) AND (#0 = #1)
          CrossJoin
            Get l81
            Get x
  cte l81 =
    Distinct group_by=[#0]
      Get l80
  cte l80 =
    Filter #54
      Join on=(#0 = #53)
        Get l77
        Union
          Get l79
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l79
                    Distinct group_by=[#0]
                      Get l78
                  Constant
                    - (false)
  cte l79 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 27) AND (#0 = #1)
          CrossJoin
            Get l78
            Get x
  cte l78 =
    Distinct group_by=[#0]
      Get l77
  cte l77 =
    Filter #52
      Join on=(#0 = #51)
        Get l74
        Union
          Get l76
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l76
                    Distinct group_by=[#0]
                      Get l75
                  Constant
                    - (false)
  cte l76 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 26) AND (#0 = #1)
          CrossJoin
            Get l75
            Get x
  cte l75 =
    Distinct group_by=[#0]
      Get l74
  cte l74 =
    Filter #50
      Join on=(#0 = #49)
        Get l71
        Union
          Get l73
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l73
                    Distinct group_by=[#0]
                      Get l72
                  Constant
                    - (false)
  cte l73 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 25) AND (#0 = #1)
          CrossJoin
            Get l72
            Get x
  cte l72 =
    Distinct group_by=[#0]
      Get l71
  cte l71 =
    Filter #48
      Join on=(#0 = #47)
        Get l68
        Union
          Get l70
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l70
                    Distinct group_by=[#0]
                      Get l69
                  Constant
                    - (false)
  cte l70 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 24) AND (#0 = #1)
          CrossJoin
            Get l69
            Get x
  cte l69 =
    Distinct group_by=[#0]
      Get l68
  cte l68 =
    Filter #46
      Join on=(#0 = #45)
        Get l65
        Union
          Get l67
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l67
                    Distinct group_by=[#0]
                      Get l66
                  Constant
                    - (false)
  cte l67 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 23) AND (#0 = #1)
          CrossJoin
            Get l66
            Get x
  cte l66 =
    Distinct group_by=[#0]
      Get l65
  cte l65 =
    Filter #44
      Join on=(#0 = #43)
        Get l62
        Union
          Get l64
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l64
                    Distinct group_by=[#0]
                      Get l63
                  Constant
                    - (false)
  cte l64 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 22) AND (#0 = #1)
          CrossJoin
            Get l63
            Get x
  cte l63 =
    Distinct group_by=[#0]
      Get l62
  cte l62 =
    Filter #42
      Join on=(#0 = #41)
        Get l59
        Union
          Get l61
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l61
                    Distinct group_by=[#0]
                      Get l60
                  Constant
                    - (false)
  cte l61 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 21) AND (#0 = #1)
          CrossJoin
            Get l60
            Get x
  cte l60 =
    Distinct group_by=[#0]
      Get l59
  cte l59 =
    Filter #40
      Join on=(#0 = #39)
        Get l56
        Union
          Get l58
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l58
                    Distinct group_by=[#0]
                      Get l57
                  Constant
                    - (false)
  cte l58 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 20) AND (#0 = #1)
          CrossJoin
            Get l57
            Get x
  cte l57 =
    Distinct group_by=[#0]
      Get l56
  cte l56 =
    Filter #38
      Join on=(#0 = #37)
        Get l53
        Union
          Get l55
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l55
                    Distinct group_by=[#0]
                      Get l54
                  Constant
                    - (false)
  cte l55 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 19) AND (#0 = #1)
          CrossJoin
            Get l54
            Get x
  cte l54 =
    Distinct group_by=[#0]
      Get l53
  cte l53 =
    Filter #36
      Join on=(#0 = #35)
        Get l50
        Union
          Get l52
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l52
                    Distinct group_by=[#0]
                      Get l51
                  Constant
                    - (false)
  cte l52 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 18) AND (#0 = #1)
          CrossJoin
            Get l51
            Get x
  cte l51 =
    Distinct group_by=[#0]
      Get l50
  cte l50 =
    Filter #34
      Join on=(#0 = #33)
        Get l47
        Union
          Get l49
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l49
                    Distinct group_by=[#0]
                      Get l48
                  Constant
                    - (false)
  cte l49 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 17) AND (#0 = #1)
          CrossJoin
            Get l48
            Get x
  cte l48 =
    Distinct group_by=[#0]
      Get l47
  cte l47 =
    Filter #32
      Join on=(#0 = #31)
        Get l44
        Union
          Get l46
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l46
                    Distinct group_by=[#0]
                      Get l45
                  Constant
                    - (false)
  cte l46 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 16) AND (#0 = #1)
          CrossJoin
            Get l45
            Get x
  cte l45 =
    Distinct group_by=[#0]
      Get l44
  cte l44 =
    Filter #30
      Join on=(#0 = #29)
        Get l41
        Union
          Get l43
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l43
                    Distinct group_by=[#0]
                      Get l42
                  Constant
                    - (false)
  cte l43 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 15) AND (#0 = #1)
          CrossJoin
            Get l42
            Get x
  cte l42 =
    Distinct group_by=[#0]
      Get l41
  cte l41 =
    Filter #28
      Join on=(#0 = #27)
        Get l38
        Union
          Get l40
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l40
                    Distinct group_by=[#0]
                      Get l39
                  Constant
                    - (false)
  cte l40 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 14) AND (#0 = #1)
          CrossJoin
            Get l39
            Get x
  cte l39 =
    Distinct group_by=[#0]
      Get l38
  cte l38 =
    Filter #26
      Join on=(#0 = #25)
        Get l35
        Union
          Get l37
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l37
                    Distinct group_by=[#0]
                      Get l36
                  Constant
                    - (false)
  cte l37 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 13) AND (#0 = #1)
          CrossJoin
            Get l36
            Get x
  cte l36 =
    Distinct group_by=[#0]
      Get l35
  cte l35 =
    Filter #24
      Join on=(#0 = #23)
        Get l32
        Union
          Get l34
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l34
                    Distinct group_by=[#0]
                      Get l33
                  Constant
                    - (false)
  cte l34 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 12) AND (#0 = #1)
          CrossJoin
            Get l33
            Get x
  cte l33 =
    Distinct group_by=[#0]
      Get l32
  cte l32 =
    Filter #22
      Join on=(#0 = #21)
        Get l29
        Union
          Get l31
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l31
                    Distinct group_by=[#0]
                      Get l30
                  Constant
                    - (false)
  cte l31 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 11) AND (#0 = #1)
          CrossJoin
            Get l30
            Get x
  cte l30 =
    Distinct group_by=[#0]
      Get l29
  cte l29 =
    Filter #20
      Join on=(#0 = #19)
        Get l26
        Union
          Get l28
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l28
                    Distinct group_by=[#0]
                      Get l27
                  Constant
                    - (false)
  cte l28 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 10) AND (#0 = #1)
          CrossJoin
            Get l27
            Get x
  cte l27 =
    Distinct group_by=[#0]
      Get l26
  cte l26 =
    Filter #18
      Join on=(#0 = #17)
        Get l23
        Union
          Get l25
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l25
                    Distinct group_by=[#0]
                      Get l24
                  Constant
                    - (false)
  cte l25 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 9) AND (#0 = #1)
          CrossJoin
            Get l24
            Get x
  cte l24 =
    Distinct group_by=[#0]
      Get l23
  cte l23 =
    Filter #16
      Join on=(#0 = #15)
        Get l20
        Union
          Get l22
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l22
                    Distinct group_by=[#0]
                      Get l21
                  Constant
                    - (false)
  cte l22 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 8) AND (#0 = #1)
          CrossJoin
            Get l21
            Get x
  cte l21 =
    Distinct group_by=[#0]
      Get l20
  cte l20 =
    Filter #14
      Join on=(#0 = #13)
        Get l17
        Union
          Get l19
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l19
                    Distinct group_by=[#0]
                      Get l18
                  Constant
                    - (false)
  cte l19 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 7) AND (#0 = #1)
          CrossJoin
            Get l18
            Get x
  cte l18 =
    Distinct group_by=[#0]
      Get l17
  cte l17 =
    Filter #12
      Join on=(#0 = #11)
        Get l14
        Union
          Get l16
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l16
                    Distinct group_by=[#0]
                      Get l15
                  Constant
                    - (false)
  cte l16 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 6) AND (#0 = #1)
          CrossJoin
            Get l15
            Get x
  cte l15 =
    Distinct group_by=[#0]
      Get l14
  cte l14 =
    Filter #10
      Join on=(#0 = #9)
        Get l11
        Union
          Get l13
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l13
                    Distinct group_by=[#0]
                      Get l12
                  Constant
                    - (false)
  cte l13 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 5) AND (#0 = #1)
          CrossJoin
            Get l12
            Get x
  cte l12 =
    Distinct group_by=[#0]
      Get l11
  cte l11 =
    Filter #8
      Join on=(#0 = #7)
        Get l8
        Union
          Get l10
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l10
                    Distinct group_by=[#0]
                      Get l9
                  Constant
                    - (false)
  cte l10 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 4) AND (#0 = #1)
          CrossJoin
            Get l9
            Get x
  cte l9 =
    Distinct group_by=[#0]
      Get l8
  cte l8 =
    Filter #6
      Join on=(#0 = #5)
        Get l5
        Union
          Get l7
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l7
                    Distinct group_by=[#0]
                      Get l6
                  Constant
                    - (false)
  cte l7 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 3) AND (#0 = #1)
          CrossJoin
            Get l6
            Get x
  cte l6 =
    Distinct group_by=[#0]
      Get l5
  cte l5 =
    Filter #4
      Join on=(#0 = #3)
        Get l2
        Union
          Get l4
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l4
                    Distinct group_by=[#0]
                      Get l3
                  Constant
                    - (false)
  cte l4 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 2) AND (#0 = #1)
          CrossJoin
            Get l3
            Get x
  cte l3 =
    Distinct group_by=[#0]
      Get l2
  cte l2 =
    Filter #2
      Join on=(#0 = #1)
        Get x
        Union
          Get l1
          Project (#0, #2)
            Project (#0, #2, #1)
              Map (#0)
                CrossJoin
                  Union
                    Negate
                      Distinct group_by=[#0]
                        Get l1
                    Distinct group_by=[#0]
                      Get l0
                  Constant
                    - (false)
  cte l1 =
    Map (true)
      Distinct group_by=[#0]
        Filter (#1 <= 1) AND (#0 = #1)
          CrossJoin
            Get l0
            Get x
  cte l0 =
    Distinct group_by=[#0]
      Get x



# ProvInfo inference (basic cases)
# --------------------------------

# WMR handling
apply pipeline=redundant_join
Return
  Distinct group_by=[#0, #1]
    Union
      Get l2
      Get l4
With Mutually Recursive
  cte l5 = // { types: "(bigint?, bigint?)" }
    Get y
  cte l4 = // { types: "(bigint?, bigint?)" }
    Return
      Get l3
    With Mutually Recursive
      cte l3 = // { types: "(bigint?, bigint?)" }
        Reduce group_by=[#0] aggregates=[min(#1)]
          Union
            Project (#0, #0)
              Get y
            Project (#0, #3)
              Join on=(#1 = #2)
                Filter (#1) IS NOT NULL
                  Get l0
                Filter (#0) IS NOT NULL
                  Get l3
  cte l2 = // { types: "(bigint?, bigint?)" }
    Return
      Get l1
    With Mutually Recursive
      cte l1 = // { types: "(bigint?, bigint?)" }
        Reduce group_by=[#0] aggregates=[min(#1)]
          Union
            Project (#1, #1)
              Get y
            Project (#1, #3)
              Join on=(#0 = #2)
                Filter (#0) IS NOT NULL
                  Get l0
                Filter (#0) IS NOT NULL
                  Get l1
  cte l0 = // { types: "(bigint?, bigint?)" }
    Union
      Threshold
        Union
          Get y
          Negate
            Get l5
      Project (#0, #1)
        Join on=(eq(#0, #2, #6) AND eq(#1, #4, #8) AND #3 = #5 AND #7 = #9)
          Filter (#0) IS NOT NULL AND (#1) IS NOT NULL
            Get y
          Filter (#0) IS NOT NULL AND (#1) IS NOT NULL
            Get l2
          Filter (#0) IS NOT NULL AND (#1) IS NOT NULL
            Get l2
          Filter (#0) IS NOT NULL AND (#1) IS NOT NULL
            Get l4
          Filter (#0) IS NOT NULL AND (#1) IS NOT NULL
            Get l4
----
Return
  Distinct group_by=[#0, #1]
    Union
      Get l2
      Get l4
With Mutually Recursive
  cte l5 =
    Get y
  cte l4 =
    Return
      Get l3
    With Mutually Recursive
      cte l3 =
        Reduce group_by=[#0] aggregates=[min(#1)]
          Union
            Project (#0, #0)
              Get y
            Project (#0, #3)
              Join on=(#1 = #2)
                Filter (#1) IS NOT NULL
                  Get l0
                Filter (#0) IS NOT NULL
                  Get l3
  cte l2 =
    Return
      Get l1
    With Mutually Recursive
      cte l1 =
        Reduce group_by=[#0] aggregates=[min(#1)]
          Union
            Project (#1, #1)
              Get y
            Project (#1, #3)
              Join on=(#0 = #2)
                Filter (#0) IS NOT NULL
                  Get l0
                Filter (#0) IS NOT NULL
                  Get l1
  cte l0 =
    Union
      Threshold
        Union
          Get y
          Negate
            Get l5
      Project (#0, #1)
        Join on=(eq(#0, #2, #6) AND eq(#1, #4, #8) AND #3 = #5 AND #7 = #9)
          Filter (#0) IS NOT NULL AND (#1) IS NOT NULL
            Get y
          Filter (#0) IS NOT NULL AND (#1) IS NOT NULL
            Get l2
          Filter (#0) IS NOT NULL AND (#1) IS NOT NULL
            Get l2
          Filter (#0) IS NOT NULL AND (#1) IS NOT NULL
            Get l4
          Filter (#0) IS NOT NULL AND (#1) IS NOT NULL
            Get l4
