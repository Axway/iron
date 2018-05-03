package io.axway.iron.assertions;

import io.axway.iron.sample.model.Company;

/**
 * Entry point for model assertions
 */
public final class Assertions {
    /**
     * Creates a new {@link CompanyAssert} instance for the given {@link Company}.
     *
     * @param company company to assert
     * @return new {@link CompanyAssert} instance
     */
    public static CompanyAssert assertThat(Company company) {
        return new CompanyAssert(company);
    }

    private Assertions() {
        // Prevent instantiation
    }
}
