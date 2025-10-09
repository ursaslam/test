Keep this generic — you’ll use it inside your existing transformer.

package com.comcast.fcs.util;

public abstract class BaseTransformer<I, O> {

    public TransformResult<O> safeTransform(I input) {
        TransformResult<O> result = new TransformResult<>();

        try {
            O output = transform(input, result);
            result.setTransformedObject(output);
        } catch (Exception e) {
            result.addError("global", e);
        }

        return result;
    }

    protected abstract O transform(I input, TransformResult<O> result);
}

🧰 2. SafeExecutor

No change.

package com.comcast.fcs.util;

public class SafeExecutor {
    public static void safeRun(String fieldName, Runnable action, TransformResult<?> result) {
        try {
            action.run();
        } catch (NullPointerException e) {
            result.addError(fieldName, e);
        } catch (Exception e) {
            result.addError(fieldName, e);
        }
    }
}

🧮 3. TransformResult

No change needed either.

package com.comcast.fcs.util;

import java.util.ArrayList;
import java.util.List;

public class TransformResult<T> {

    private final List<String> errors = new ArrayList<>();
    private T transformedObject;

    public void addError(String fieldName, Exception e) {
        errors.add("[" + fieldName + "] failed: " + e.getMessage());
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setTransformedObject(T transformedObject) {
        this.transformedObject = transformedObject;
    }

    public T getTransformedObject() {
        return transformedObject;
    }
}

🧠 4. DCMPartyTransformer (final version)
package com.comcast.dcm.transformer;

import com.honda.party.Party as HondaParty;
import com.comcast.dcm.model.Party as DcmParty;
import com.comcast.fcs.util.SafeExecutor;
import com.comcast.fcs.util.TransformResult;
import com.comcast.fcs.util.BaseTransformer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DCMPartyTransformer implements Transformer<DcmParty> {

    private final DCMPersonTransformer personTransformer;
    private final DCMAddressTransformer addressTransformer;
    private final DCMPhoneTransformer phoneTransformer;
    private final DCMProducerTransformer producerTransformer;
    private final DCMPartyExtensionTransformer partyExtensionTransformer;
    private final DCMEMailAddressTransformer eMailAddressTransformer;
    private final DCMDrLpsTransformer drLpsTransformer;

    @Autowired
    public DCMPartyTransformer(
            DCMPersonTransformer personTransformer,
            DCMAddressTransformer addressTransformer,
            DCMPhoneTransformer phoneTransformer,
            DCMProducerTransformer producerTransformer,
            DCMPartyExtensionTransformer partyExtensionTransformer,
            DCMEMailAddressTransformer eMailAddressTransformer,
            DCMDrLpsTransformer drLpsTransformer) {

        this.personTransformer = personTransformer;
        this.addressTransformer = addressTransformer;
        this.phoneTransformer = phoneTransformer;
        this.producerTransformer = producerTransformer;
        this.partyExtensionTransformer = partyExtensionTransformer;
        this.eMailAddressTransformer = eMailAddressTransformer;
        this.drLpsTransformer = drLpsTransformer;
    }

    // The execute() method comes from your existing Transformer interface
    @Override
    public HondaParty execute(DcmParty response) {
        // internally delegate to a local base transformer that handles safety
        DCMPartyBaseSafeTransformer safeTransformer = new DCMPartyBaseSafeTransformer();
        TransformResult<HondaParty> result = safeTransformer.safeTransform(response);

        if (!result.getErrors().isEmpty()) {
            System.err.println("Transformation completed with errors: " + result.getErrors());
        }

        return result.getTransformedObject();
    }

    /**
     * Inner class to reuse BaseTransformer pattern safely
     */
    private class DCMPartyBaseSafeTransformer extends BaseTransformer<DcmParty, HondaParty> {
        @Override
        protected HondaParty transform(DcmParty source, TransformResult<HondaParty> result) {
            HondaParty target = new HondaParty();

            if (source != null) {
                try {
                    target.setPartyType("Person");
                    target.setPartyId(source.getGuid());
                    target.setFullName(source.getPerson() != null ? source.getPerson().getFullName() : null);

                    // safely run nested transformers
                    SafeExecutor.safeRun("person", () ->
                        target.setPerson(personTransformer.execute(source.getPerson())),
                        result
                    );

                    SafeExecutor.safeRun("address", () ->
                        target.setAddresses(addressTransformer.execute(source.getAddresses())),
                        result
                    );

                    SafeExecutor.safeRun("phone", () ->
                        target.setPhones(phoneTransformer.execute(source.getPhones())),
                        result
                    );

                    SafeExecutor.safeRun("producer", () ->
                        target.setProducer(producerTransformer.execute(source.getProducer())),
                        result
                    );

                    SafeExecutor.safeRun("extension", () ->
                        target.setExtensions(partyExtensionTransformer.execute(source.getPartyExtensions())),
                        result
                    );

                    SafeExecutor.safeRun("email", () ->
                        target.setEmails(eMailAddressTransformer.execute(source.getEmailAddresses())),
                        result
                    );

                    SafeExecutor.safeRun("drLps", () ->
                        target.setDrLps(drLpsTransformer.execute(source.getDrLps())),
                        result
                    );

                } catch (Exception e) {
                    result.addError("global", e);
                }
            }

            return target;
        }
    }
}
