package io.emeraldpay.dshackle.config

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldBeIn
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe

class UpstreamsConfigTest :
    ShouldSpec({

        context("Labels") {

            context("Equal") {
                should("equal empty labels") {
                    val labels1 = UpstreamsConfig.Labels()
                    val labels2 = UpstreamsConfig.Labels()

                    val equal = labels1 == labels2 && labels2 == labels1

                    equal shouldBe true
                }

                should("equal single label") {
                    val labels1 =
                        UpstreamsConfig.Labels().also {
                            it["foo"] = "bar"
                        }
                    val labels2 =
                        UpstreamsConfig.Labels().also {
                            it["foo"] = "bar"
                        }

                    val equal = labels1 == labels2 && labels2 == labels1

                    equal shouldBe true
                }
                should("equal multiple labels") {
                    val labels1 =
                        UpstreamsConfig.Labels().also {
                            it["bar"] = "baz"
                            it["foo"] = "bar"
                        }
                    val labels2 =
                        UpstreamsConfig.Labels().also {
                            it["foo"] = "bar"
                            it["bar"] = "baz"
                        }

                    val equal = labels1 == labels2 && labels2 == labels1

                    equal shouldBe true
                }

                should("not equal single label") {
                    val labels1 =
                        UpstreamsConfig.Labels().also {
                            it["foo"] = "bar"
                        }
                    val labels2 = UpstreamsConfig.Labels()

                    val equal1 = labels1 == labels2
                    equal1 shouldNotBe true

                    val equal2 = labels2 == labels1
                    equal2 shouldNotBe true
                }

                should("not equal multiple label") {
                    val labels1 =
                        UpstreamsConfig.Labels().also {
                            it["foo"] = "bar"
                        }
                    val labels2 =
                        UpstreamsConfig.Labels().also {
                            it["foo"] = "bar"
                            it["bar"] = "baz"
                        }

                    val equal1 = labels1 == labels2
                    equal1 shouldNotBe true

                    val equal2 = labels2 == labels1
                    equal2 shouldNotBe true
                }
            }

            context("toString") {
                should("empty for no labels") {
                    val labels = UpstreamsConfig.Labels()

                    labels.toString() shouldBe "[]"
                }
                should("value for one label") {
                    val labels =
                        UpstreamsConfig.Labels().also {
                            it["foo"] = "bar"
                        }

                    labels.toString() shouldBe "[foo=bar]"
                }
                should("value for two label") {
                    val labels =
                        UpstreamsConfig.Labels().also {
                            it["foo"] = "bar"
                            it["bar"] = "baz"
                        }

                    labels.toString() shouldBeIn listOf("[foo=bar, bar=baz]", "[bar=baz, foo=bar]")
                }
            }
        }
    })
