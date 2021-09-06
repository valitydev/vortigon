package com.rbkmoney.vortigon.handler.party.contract

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.geck.common.util.TBaseUtil
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.vortigon.domain.db.enums.ContractStatus
import com.rbkmoney.vortigon.domain.db.enums.ContractorType
import com.rbkmoney.vortigon.domain.db.enums.LegalEntity
import com.rbkmoney.vortigon.domain.db.enums.PrivateEntity
import com.rbkmoney.vortigon.domain.db.tables.pojos.Contract
import com.rbkmoney.vortigon.domain.db.tables.pojos.Contractor
import com.rbkmoney.vortigon.extension.getClaimStatus
import com.rbkmoney.vortigon.handler.ChangeHandler
import com.rbkmoney.vortigon.handler.constant.HandleEventType
import com.rbkmoney.vortigon.repository.ContractDao
import com.rbkmoney.vortigon.repository.ContractorDao
import mu.KotlinLogging
import org.springframework.stereotype.Component
import java.util.UUID

private val log = KotlinLogging.logger {}

@Component
class ContractCreateHandler(
    private val contractDao: ContractDao,
    private val contractorDao: ContractorDao,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        val claimStatus = change.getClaimStatus()?.accepted?.effects?.filter {
            it.isSetContractEffect && it.contractEffect.effect.isSetCreated
        }?.forEach { claimEffect ->
            val contractCreated = claimEffect.contractEffect.effect.created
            val contract = Contract().apply {
                partyId = event.sourceId
                eventId = event.eventId
                eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
                createdAt = TypeUtil.stringToLocalDateTime(contractCreated.createdAt)
                validSince = TypeUtil.stringToLocalDateTime(contractCreated.validSince)
                validUntil = TypeUtil.stringToLocalDateTime(contractCreated.validUntil)
                status = TBaseUtil.unionFieldToEnum(contractCreated.getStatus(), ContractStatus::class.java)
                if (contractCreated.status.isSetTerminated) {
                    statusTerminatedAt = TypeUtil.stringToLocalDateTime(contractCreated.status.terminated.terminatedAt)
                }
                if (contractCreated.isSetLegalAgreement) {
                    legalAgreementId = contractCreated.legal_agreement.legalAgreementId
                    legalAgreementSignedAt = TypeUtil.stringToLocalDateTime(contractCreated.legal_agreement.signedAt)
                    if (contractCreated.legal_agreement.isSetValidUntil) {
                        legalAgreementValidUntil =
                            TypeUtil.stringToLocalDateTime(contractCreated.legal_agreement.validUntil)
                    }
                }
            }
            val contractor = contractCreated.contractor
            val contractorId = getContractorId(contractCreated)
            if (contractor != null) {
                val contractor = convertThriftContractor(event, contractor)
                contractor.contractorId = contractorId
                contractorDao.save(contractor)
            }
            contract.contractorId = contractorId
            contract.contractId = claimEffect.contractEffect.contractId
            contractDao.save(contract)
        }
    }

    override fun accept(change: PartyChange): Boolean {
        if (HandleEventType.CLAIM_CREATED_FILTER.filter.match(change) ||
            HandleEventType.CLAIM_STATUS_CHANGED_FILTER.filter.match(change)
        ) {
            val claimStatus = change.getClaimStatus()
            return claimStatus?.accepted?.effects?.any {
                it.isSetContractEffect && it.contractEffect.effect.isSetCreated
            } ?: false
        }
        return false
    }

    private fun convertThriftContractor(
        event: MachineEvent,
        contractor: com.rbkmoney.damsel.domain.Contractor,
    ): Contractor {
        return Contractor().apply {
            this.partyId = event.sourceId
            eventId = event.eventId
            eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
            this.contractorId = contractorId
            contractorType = TBaseUtil.unionFieldToEnum(contractor, ContractorType::class.java)
            if (contractor.isSetRegisteredUser) {
                regUserEmail = contractor.registeredUser.email
            } else if (contractor.isSetLegalEntity) {
                legalEntityType = TBaseUtil.unionFieldToEnum(contractor.legalEntity, LegalEntity::class.java)
                if (contractor.legalEntity.isSetRussianLegalEntity) {
                    val russianLegalEntity = contractor.legalEntity.russianLegalEntity
                    russianLegalEntityName = russianLegalEntity.registeredName
                    russianLegalEntityRegisteredNumber = russianLegalEntity.registeredNumber
                    russianLegalEntityInn = russianLegalEntity.inn
                    russianLegalEntityActualAddress = russianLegalEntity.actualAddress
                    russianLegalEntityPostAddress = russianLegalEntity.postAddress
                    russianLegalEntityRepresentativePosition = russianLegalEntity.representativePosition
                    russianLegalEntityRepresentativeFullName = russianLegalEntity.representativeFullName
                    russianLegalEntityRepresentativeDocument = russianLegalEntity.representativeDocument
                    russianLegalEntityBankAccount = russianLegalEntity.russianBankAccount.account
                    russianLegalEntityBankName = russianLegalEntity.russianBankAccount.bankName
                    russianLegalEntityBankPostAccount = russianLegalEntity.russianBankAccount.bankPostAccount
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
                privateEntityType =
                    TBaseUtil.unionFieldToEnum(contractor.privateEntity, PrivateEntity::class.java)
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

    private fun getContractorId(contractCreated: com.rbkmoney.damsel.domain.Contract): String {
        val contractorId: String = if (contractCreated.isSetContractorId) {
            return contractCreated.contractorId
        } else if (contractCreated.isSetContractor) {
            return UUID.randomUUID().toString()
        } else ""
        return contractorId
    }
}
