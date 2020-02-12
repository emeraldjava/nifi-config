package com.github.hermannpencole.nifi.config;

import com.github.hermannpencole.nifi.config.model.ConfigException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;
import javax.validation.*;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * https://stackoverflow.com/questions/36089676/use-validated-and-valid-with-spring-validator
 * https://stackoverflow.com/questions/5142065/jsr-303-valid-annotation-not-working-for-list-of-child-objects
 *
 * https://www.baeldung.com/spring-value-defaults
 * https://mkyong.com/spring/spring-propertysources-example/
 * https://www.petrikainulainen.net/programming/spring-framework/spring-from-the-trenches-injecting-property-values-into-configuration-beans/
 */
@Configuration
@PropertySources({
    @PropertySource("classpath:nifi.properties"),
    @PropertySource(value="classpath:test-nifi.properties", ignoreResourceNotFound=true)
})
public class NifiClientProperties {

    @Autowired
    private Environment env;

    // The mandatory parameters
    @Value("${nifi.url:#{null}}")
    @NotBlank(message = "The 'nifi.url' property must be defined.")
    public String url;

    @Value("${nifi.username}")
    @NotEmpty
    public String username;

    @Value("${nifi.password}")
    @NotEmpty
    public String password;

    // The default parameters
    @Value("${nifi.branch}")
    @NotBlank(message = "The branch property must begin with the element 'root' ( sample : root > branch > sub-branch).")
    public String branch;

    public List<String> getBranchList() throws ConfigException {
        List<String> branchList = Arrays.stream(branch.split(">")).map(String::trim).collect(Collectors.toList());
        if (!branchList.get(0).equals("root")) {
            throw new ConfigException("The branch address must begin with the element 'root' ( sample : root > branch > sub-branch)");
        }
        return branchList;
    }

    @Value("${nifi.command:updateConfig}")
    public String command = "updateConfig";

    @Value("${nifi.interval:2}")
    public int interval;

    @Value("${nifi.connectionTimeout:10000}")
    public Integer connectionTimeout;

    @Value("${nifi.placeWidth:1935d}")
    public Double placeWidth;

    @Value("${nifi.startPlace:0,0}")
    public String startPlace;

    @Value("${nifi.forceMode:false}")
    public Boolean forceMode;

    @Value("${nifi.verifySsl:false}")
    public Boolean verifySsl;

    @Value("${nifi.debugMode:false}")
    public Boolean debugMode;

    @Value("${nifi.timeout:120}")
    public int timeout;

    /**
     * https://stackoverflow.com/questions/49099646/jsr-303-annotations-do-not-validate-bean-properties-when-spring-boot-application
     */
    @PostConstruct
    public void init() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        final Set<ConstraintViolation<NifiClientProperties>> validationErrors = validator.validate(this);
        if (!validationErrors.isEmpty()) {
            throw new ValidationException("Property Validation Errors : " + validationErrors);
        }
    }
}
