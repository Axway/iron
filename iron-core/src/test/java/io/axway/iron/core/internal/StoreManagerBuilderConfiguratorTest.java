package io.axway.iron.core.internal;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.google.common.collect.ImmutableMap;

import static org.assertj.core.api.Assertions.assertThat;

public class StoreManagerBuilderConfiguratorTest {

    //region getProperty

    @DataProvider(name = "getPropertyDataProvider")
    public Object[][] getPropertyDataProvider() throws Exception {
        System.setProperty("mySysProperty", "mySysValue");
        setEnv(ImmutableMap.of("MY_VAR_ENV", "myVarEnvValue"));
        return new Object[][]{//
                {"no variable", "myProperty=myValue", "myProperty", "myValue"},//
                {"a single variable", "myProperty=${otherProperty}\notherProperty=myOtherPropertyValue", "myProperty", "myOtherPropertyValue"},//
                {"a single var env", "myProperty=${env:MY_VAR_ENV}", "myProperty", "myVarEnvValue"},//
                {"a single system property", "myProperty=${sys:mySysProperty}", "myProperty", "mySysValue"},//

                {"a variable + rest", "myProperty=${otherProperty}/rest\notherProperty=myOtherPropertyValue", "myProperty", "myOtherPropertyValue/rest"},//
                {"a var env + rest", "myProperty=${env:MY_VAR_ENV}/rest", "myProperty", "myVarEnvValue/rest"},//
                {"a system property + rest", "myProperty=${sys:mySysProperty}/rest", "myProperty", "mySysValue/rest"},//

                {"full complex example",
                        "myProperty=prefix/${sys:mySysProperty}/sysToEnv/${env:MY_VAR_ENV}/envToProp/${otherProperty}/rest\notherProperty=myOtherPropertyValue",
                        "myProperty", "prefix/mySysValue/sysToEnv/myVarEnvValue/envToProp/myOtherPropertyValue/rest"},//
                {"full complex example",
                        "myProperty=prefix/${SYS:mySysProperty}/sysToEnv/${ENV:MY_VAR_ENV}/envToProp/${otherProperty}/rest\notherProperty=myOtherPropertyValue",
                        "myProperty", "prefix/mySysValue/sysToEnv/myVarEnvValue/envToProp/myOtherPropertyValue/rest"},//

        };
    }

    @Test(dataProvider = "getPropertyDataProvider")
    public void test(String message, String mapProperties, String key, String expectedValue) throws IOException {
        Properties properties = new Properties();
        properties.load(new StringReader(mapProperties));
        assertThat(StoreManagerBuilderConfigurator.getProperty(properties, key)).as(message).isEqualTo(expectedValue);
    }

    private static void setEnv(Map<String, String> newenv) throws Exception {
        try {
            Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
            theEnvironmentField.setAccessible(true);
            Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
            env.putAll(newenv);
            Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
            theCaseInsensitiveEnvironmentField.setAccessible(true);
            Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
            cienv.putAll(newenv);
        } catch (NoSuchFieldException e) {
            Class[] classes = Collections.class.getDeclaredClasses();
            Map<String, String> env = System.getenv();
            for (Class cl : classes) {
                if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
                    Field field = cl.getDeclaredField("m");
                    field.setAccessible(true);
                    Object obj = field.get(env);
                    Map<String, String> map = (Map<String, String>) obj;
                    map.clear();
                    map.putAll(newenv);
                }
            }
        }
    }
    //endregion
}
