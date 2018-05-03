package io.axway.iron.assertions;

import java.util.*;
import org.assertj.core.api.AbstractAssert;
import io.axway.iron.sample.model.Company;

/**
 * This class helps testing the state of of {@link Company} instance.
 *
 * @see Assertions#assertThat(Company)
 */
@SuppressWarnings("UnusedReturnValue")
public final class CompanyAssert extends AbstractAssert<CompanyAssert, Company> {
    CompanyAssert(Company company) {
        super(company, CompanyAssert.class);
    }

    /**
     * Checks that the company has the given name
     *
     * @param companyName expected company name
     * @return self instance, fluent API
     */
    public CompanyAssert hasName(String companyName) {
        isNotNull();
        if (!Objects.equals(actual.name(), companyName)) {
            failWithMessage("Expected company name to be <%s> but was <%s>", companyName, actual.name());
        }
        return this;
    }

    /**
     * Checks that the company has the given address
     *
     * @param address expected company address
     * @return self instance, fluent API
     */
    public CompanyAssert hasAddress(String address) {
        isNotNull();
        if (!Objects.equals(actual.address(), address)) {
            failWithMessage("Expected company address to be <%s> but was <%s>", address, actual.address());
        }
        return this;
    }

    /**
     * Checks that teh company has the given country
     *
     * @param country expected company address
     * @return self instance, fluent API
     */
    public CompanyAssert hasCountry(String country) {
        isNotNull();
        if (!Objects.equals(actual.country(), country)) {
            failWithMessage("Expected company country to be <%s> but was <%s>", country, actual.country());
        }
        return this;
    }
}
