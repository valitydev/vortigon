package dev.vality.vortigon.handler.converter

import dev.vality.damsel.domain.Contractor
import dev.vality.geck.common.util.TBaseUtil
import org.springframework.stereotype.Component

@Component
class ContractorToRecordConverter :
    DomainConverter<Contractor, dev.vality.vortigon.domain.db.tables.pojos.Contractor> {

    override fun convert(contractor: Contractor): dev.vality.vortigon.domain.db.tables.pojos.Contractor {
        return dev.vality.vortigon.domain.db.tables.pojos.Contractor().apply {
            contractorType = TBaseUtil.unionFieldToEnum(contractor, dev.vality.vortigon.domain.db.enums.ContractorType::class.java)
            if (contractor.isSetRegisteredUser) {
                regUserEmail = contractor.registeredUser.getEmail()
            } else if (contractor.isSetLegalEntity) {
                if (contractor.legalEntity.isSetRussianLegalEntity) {
                    val russianLegalEntity = contractor.legalEntity.russianLegalEntity
                    legalEntityType = TBaseUtil.unionFieldToEnum(contractor.legalEntity, dev.vality.vortigon.domain.db.enums.LegalEntity::class.java)
                    russianLegalEntityName = russianLegalEntity.registeredName
                    russianLegalEntityRegisteredNumber = russianLegalEntity.registeredNumber
                    russianLegalEntityInn = russianLegalEntity.inn
                    russianLegalEntityActualAddress = russianLegalEntity.actualAddress
                    russianLegalEntityPostAddress = russianLegalEntity.postAddress
                    russianLegalEntityRepresentativePosition = russianLegalEntity.representativePosition
                    russianLegalEntityRepresentativeDocument = russianLegalEntity.representativeDocument
                    russianLegalEntityRepresentativeFullName = russianLegalEntity.representativeFullName
                    russianLegalEntityBankAccount = russianLegalEntity.russianBankAccount.account
                    russianLegalEntityBankName = russianLegalEntity.russianBankAccount.bankName
                    russianLegalEntityBankPostAccount = russianLegalEntity.russianBankAccount.bankPostAccount
                    russianLegalEntityBankBik = russianLegalEntity.russianBankAccount.bankBik
                } else if (contractor.legalEntity.isSetInternationalLegalEntity) {
                    val internationalLegalEntity = contractor.legalEntity.internationalLegalEntity
                    internationalLegalEntityName = internationalLegalEntity.legalName
                    internationalLegalEntityTradingName = internationalLegalEntity.tradingName
                    internationalLegalEntityRegisteredAddress = internationalLegalEntity.registeredAddress
                    internationalActualAddress = internationalLegalEntity.actualAddress
                    internationalLegalEntityRegisteredNumber = internationalLegalEntity.registeredNumber
                    if (internationalLegalEntity.isSetCountry) {
                        internationalLegalEntityCountryCode = internationalLegalEntity.country.id.name
                    }
                }
            } else if (contractor.isSetPrivateEntity) {
                privateEntityType = TBaseUtil.unionFieldToEnum(contractor.privateEntity, dev.vality.vortigon.domain.db.enums.PrivateEntity::class.java)
                if (contractor.privateEntity.isSetRussianPrivateEntity) {
                    val russianPrivateEntity = contractor.privateEntity.russianPrivateEntity
                    if (russianPrivateEntity.isSetContactInfo) {
                        russianPrivateEntityEmail = russianPrivateEntity.contactInfo.email
                        russianPrivateEntityPhoneNumber = russianPrivateEntity.contactInfo.phone_number
                    }
                    russianPrivateEntityFirstName = russianPrivateEntity.firstName
                    russianPrivateEntitySecondName = russianPrivateEntity.secondName
                    russianPrivateEntityMiddleName = russianPrivateEntity.middleName
                }
            }
        }
    }
}
