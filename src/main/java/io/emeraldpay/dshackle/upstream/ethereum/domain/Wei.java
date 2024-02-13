/*
 * Copyright (c) 2020 EmeraldPay Inc, All Rights Reserved.
 * Copyright (c) 2016-2017 Infinitape Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.emeraldpay.dshackle.upstream.ethereum.domain;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * Wei amount.
 */
public class Wei implements Serializable, Comparable<Wei> {

    /**
     * Wei denomination units.
     */
    public enum Unit {

        WEI("wei", 0),
        KWEI("Kwei", 3),
        MWEI("Mwei", 6),
        GWEI("Gwei", 9),
        SZABO("szabo", 12),
        FINNEY("finney", 15),
        ETHER("ether", 18),
        KETHER("Kether", 21),
        METHER("Mether", 24);

        private String name;

        private int scale;

        /**
         * @param name  a unit name
         * @param scale a wei base multiplication factor expressed as a degree of power ten
         */
        Unit(String name, int scale) {
            this.name = name;
            this.scale = scale;
        }

        /**
         * @return a unit name
         */
        public String getName() {
            return name;
        }

        /**
         * @return a wei base multiplication factor expressed as a degree of power ten
         */
        public int getScale() {
            return scale;
        }
    }

    public final static Wei ZERO = new Wei();

    /**
     * @param val amount in {@link Unit#ETHER}
     * @return corresponding amount in wei
     * @see #ofUnits(double, Unit)
     */
    public static Wei ofEthers(double val) {
        return ofUnits(val, Unit.ETHER);
    }

    /**
     * @param num amount in {@link Unit#ETHER}
     * @return corresponding amount in wei
     * @see #ofUnits(BigDecimal, Unit)
     */
    public static Wei ofEthers(BigDecimal num) {
        return ofUnits(num, Unit.ETHER);
    }

    /**
     * @param val amount in some custom denomination {@link Unit}
     * @param unit target unit
     * @return corresponding amount in wei
     */
    public static Wei ofUnits(double val, Unit unit) {
        return ofUnits(BigDecimal.valueOf(val), unit);
    }

    /**
     * @param num amount in some custom denomination {@link Unit}
     * @param unit target unit
     * @return corresponding amount in wei
     */
    public static Wei ofUnits(BigDecimal num, Unit unit) {
        return new Wei(num.scaleByPowerOfTen(unit.getScale()).toBigInteger());
    }

    /**
     * Parse hex representation of the amount, like 0x100
     *
     * @param value string representation of the amount
     * @return parsed value or exception
     * @throws IllegalArgumentException if passed value is null or not hex
     */
    public static Wei from(String value) { //TODO rename to fromHex?
        //TODO return null?
        if (value == null) {
            throw new IllegalArgumentException("Null Address");
        }
        if (!value.startsWith("0x") || value.length() <= 2) {
            throw new IllegalArgumentException("Invalid hex format: " + value);
        }
        value = value.substring(2);
        return new Wei(new BigInteger(value, 16));
    }

    private final BigInteger amount;

    /**
     * Create zero wei amount.
     */
    public Wei() {
        this(BigInteger.ZERO);
    }

    /**
     * @param val an amount in wei
     */
    public Wei(long val) {
        this(BigInteger.valueOf(val));
    }

    /**
     * @param num an amount in wei
     */
    public Wei(BigInteger num) {
        this.amount = Objects.requireNonNull(num);
    }

    /**
     * @return an amount in wei
     */
    public BigInteger getAmount() {
        return amount;
    }

    public Wei plus(Wei another) {
        Objects.requireNonNull(another);
        return new Wei(amount.add(another.amount));
    }

    public Wei minus(Wei another) {
        Objects.requireNonNull(another);
        return new Wei(amount.subtract(another.amount));
    }

    public Wei multiply(Long multiplier) {
        Objects.requireNonNull(multiplier);
        return new Wei(amount.multiply(BigInteger.valueOf(multiplier)));
    }

    public Wei multiply(Integer multiplier) {
        Objects.requireNonNull(multiplier);
        return new Wei(amount.multiply(BigInteger.valueOf(multiplier)));
    }

    public Wei div(Long divider) {
        Objects.requireNonNull(divider);
        return new Wei(amount.divide(BigInteger.valueOf(divider)));
    }

    public Wei div(Integer divider) {
        Objects.requireNonNull(divider);
        return new Wei(amount.divide(BigInteger.valueOf(divider)));
    }

    /**
     * @param decimalPlaces scale of the {@code BigDecimal} value to be returned
     * @return corresponding amount in {@link Unit#ETHER}
     * @see #toUnits(Unit, int)
     */
    public BigDecimal toEthers(int decimalPlaces) {
        return toUnits(Unit.ETHER, decimalPlaces);
    }

    /**
     * @return corresponding amount in {@link Unit#ETHER}
     * @see #toUnits(Unit)
     */
    public BigDecimal toEthers() {
        return toUnits(Unit.ETHER);
    }

    /**
     * @param decimalPlaces scale of the {@code BigDecimal} value to be returned
     * @param unit target unit
     * @return corresponding amount in custom denomination {@link Unit}
     * @see #toUnits(Unit, int)
     */
    public BigDecimal toUnits(Unit unit, int decimalPlaces) {
        return toUnits(unit).setScale(decimalPlaces, RoundingMode.HALF_UP);
    }

    /**
     * @return corresponding amount in custom denomination {@link Unit}
     * @param unit target unit
     * @see #toUnits(Unit)
     */
    public BigDecimal toUnits(Unit unit) {
        return new BigDecimal(amount).scaleByPowerOfTen(-unit.getScale());
    }

    @Override
    public int compareTo(Wei o) {
        return amount.compareTo(o.amount);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), amount);
    }

    @Override
    public final boolean equals(Object obj) {
        if (!(obj instanceof Wei)) return false;

        Wei other = (Wei) obj;

        return Objects.equals(amount, other.amount);
    }

    @Override
    public String toString() {
        return String.format("%s wei", amount.toString());
    }

    public String toHex() {
        return "0x" + amount.toString(16);
    }
}
