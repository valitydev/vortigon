package com.rbkmoney.vortigon.handler.converter

import com.rbkmoney.damsel.domain.Contractor
import com.rbkmoney.geck.common.util.TBaseUtil
import com.rbkmoney.vortigon.domain.db.enums.ContractorType
import com.rbkmoney.vortigon.domain.db.enums.LegalEntity
import com.rbkmoney.vortigon.domain.db.enums.PrivateEntity
import org.springframework.stereotype.Component

@Component
class ContractorToRecordConverter :
    DomainConverter<Contractor, com.rbkmoney.vortigon.domain.db.tables.pojos.Contractor> {

    override fun convert(contractor: Contractor): com.rbkmoney.vortigon.domain.db.tables.pojos.Contractor {
        return com.rbkmoney.vortigon.domain.db.tables.pojos.Contractor().apply {
            contractorType = TBaseUtil.unionFieldToEnum(contractor, ContractorType::class.java)
            if (contractor.isSetRegisteredUser) {
                regUserEmail = contractor.registeredUser.getEmail()
            } else if (contractor.isSetLegalEntity) {
                if (contractor.legalEntity.isSetRussianLegalEntity) {
                    val russianLegalEntity = contractor.legalEntity.russianLegalEntity
                    legalEntityType = TBaseUtil.unionFieldToEnum(contractor.legalEntity, LegalEntity::class.java)
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
                privateEntityType = TBaseUtil.unionFieldToEnum(contractor.privateEntity, PrivateEntity::class.java)
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
