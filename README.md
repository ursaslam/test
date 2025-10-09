import java.util.ArrayList;
import java.util.List;

public class TransformResult<T> {
    private T transformedObject;
    private final List<String> errors = new ArrayList<>();

    public void addError(String field, Exception e) {
        errors.add("Field '" + field + "' failed: " + e.getMessage());
    }

    public void setTransformedObject(T obj) {
        this.transformedObject = obj;
    }

    public T getTransformedObject() {
        return transformedObject;
    }

    public List<String> getErrors() {
        return errors;
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }
}


public abstract class BaseTransformer<T> {

    public TransformResult<T> safeTransform(T input) {
        TransformResult<T> result = new TransformResult<>();

        try {
            transform(input, result); // delegate actual logic
            result.setTransformedObject(input);
        } catch (Exception e) {
            // Catch any unhandled error from child transformer
            result.addError("global", e);
        }

        return result;
    }

    // Each subclass implements this with field-level transformations
    protected abstract void transform(T input, TransformResult<T> result);
}

public class SafeExecutor {
    public static <T> void safeRun(String fieldName, Runnable action, TransformResult<T> result) {
        try {
            action.run();
        } catch (NullPointerException e) {
            result.addError(fieldName, e);
        } catch (Exception e) {
            result.addError(fieldName, e);
        }
    }
}

public class AddressTransformer extends BaseTransformer<Address> {
    @Override
    protected void transform(Address address, TransformResult<Address> result) {
        SafeExecutor.safeRun("city", () -> {
            address.setCity(address.getCity().trim().toUpperCase());
        }, result);

        SafeExecutor.safeRun("zip", () -> {
            address.setZip(address.getZip().replaceAll("-", ""));
        }, result);
    }
}

public class CustomerTransformer extends BaseTransformer<Customer> {
    private final AddressTransformer addressTransformer = new AddressTransformer();

    @Override
    
    protected void transform(Customer customer, TransformResult<Customer> result) {
        SafeExecutor.safeRun("name", () -> {
            customer.setName(customer.getName().toUpperCase());
        }, result);

        SafeExecutor.safeRun("age", () -> {
            customer.setAge(customer.getAge() + 1);
        }, result);

        // Nested transformer call
        SafeExecutor.safeRun("address", () -> {
            TransformResult<Address> addrResult = addressTransformer.safeTransform(customer.getAddress());
            result.getErrors().addAll(addrResult.getErrors());
        }, result);
    }
}
